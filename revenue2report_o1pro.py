import streamlit as st
import json
import datetime
import re
import time
import io
import zipfile
import requests as req
import unicodedata

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# gspread-formatting
from gspread_formatting import (
    format_cell_range,
    CellFormat,
    Color,
    TextFormat,
    set_column_width,
    set_row_height
)

from collections import defaultdict

# (신규) openpyxl
import openpyxl
from openpyxl import Workbook


# ========== [1] 인증/초기설정 =============
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

def get_credentials_from_secrets(which: str = "A") -> Credentials:
    """
    which="A"  -> st.secrets["google_service_account_a"] 사용
    which="B"  -> st.secrets["google_service_account_b"] 사용
    """
    if which.upper() == "A":
        service_account_info = st.secrets["google_service_account_a"]
    else:
        service_account_info = st.secrets["google_service_account_b"]

    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=SCOPES
    )
    return credentials



# ----------------------------------------------------------------
# 검증(비교) 및 기타 헬퍼
# ----------------------------------------------------------------

def debug_hex(s: str) -> str:
    """문자열 s의 각 문자를 유니코드 코드포인트(\\uXXXX) 형태로 변환."""
    return " ".join(f"\\u{ord(ch):04X}" for ch in s)


def clean_artist_name(raw_name: str) -> str:
    """
    1) 유니코드 정규화(NFKC)
    2) 모든 제어문자(Category=C) 제거
    3) \xa0, \u3000 같은 특수 공백 치환
    4) strip()
    """
    import unicodedata
    if not raw_name:
        return ""

    # 1) 유니코드 정규화
    normalized = unicodedata.normalize('NFKC', raw_name)

    # 2) "모든 제어문자" 제거 (제어문자: Cc, Cf, Cs, Co, Cn 등)
    no_ctrl = "".join(ch for ch in normalized if not unicodedata.category(ch).startswith("C"))

    # 3) 특수공백 치환 + strip
    no_ctrl = no_ctrl.replace('\xa0',' ').replace('\u3000',' ')
    cleaned = no_ctrl.strip()

    return cleaned

def show_detailed_verification():
    check_dict = st.session_state.get("check_dict", {})
    dv = check_dict.get("details_verification", {})
    if not dv:
        st.warning("세부 검증 데이터가 없습니다.")
        return

    tabA, tabB = st.tabs(["정산서 검증", "세부매출 검증"])

    with tabA:
        st.write("#### 정산서 검증")
        rows = dv.get("정산서", [])
        if not rows:
            st.info("정산서 검증 데이터가 없습니다.")
        else:
            import pandas as pd
            df = pd.DataFrame(rows)

            bool_cols = [c for c in df.columns if c.startswith("match_")]

            def highlight_boolean(val):
                if val is True:
                    return "background-color: #AAFFAA"
                elif val is False:
                    return "background-color: #FFAAAA"
                else:
                    return ""

            int_columns = [
                "원본_곡비", "정산서_곡비",
                "원본_공제금액", "정산서_공제금액",
                "원본_공제후잔액", "정산서_공제후잔액",
                "원본_정산율(%)", "정산서_정산율(%)"
            ]
            format_dict = {col: "{:.0f}" for col in int_columns if col in df.columns}

            st.dataframe(
                df.style
                  .format(format_dict)
                  .applymap(highlight_boolean, subset=bool_cols)
            )

    with tabB:
        st.write("#### 세부매출 검증")
        rows = dv.get("세부매출", [])
        if not rows:
            st.info("세부매출 검증 데이터가 없습니다.")
        else:
            import pandas as pd
            df = pd.DataFrame(rows)
            bool_cols = [c for c in df.columns if c.startswith("match_")]

            def highlight_boolean(val):
                if val is True:
                    return "background-color: #AAFFAA"
                elif val is False:
                    return "background-color: #FFAAAA"
                else:
                    return ""

            int_columns = ["원본_매출액", "정산서_매출액"]
            format_dict = {col: "{:.0f}" for col in int_columns if col in df.columns}

            st.dataframe(
                df.style
                  .format(format_dict)
                  .applymap(highlight_boolean, subset=bool_cols)
            )


def compare_artists(song_artists, revenue_artists):
    set_song = set(song_artists)
    set_revenue = set(revenue_artists)
    return {
        "missing_in_song": sorted(set_revenue - set_song),
        "missing_in_revenue": sorted(set_song - set_revenue),
        "common_count": len(set_song & set_revenue),
        "song_count": len(set_song),
        "revenue_count": len(set_revenue),
    }

def normalized_month(m):
    m = m.strip()
    if re.match(r'^\d{6}$', m):  # 202412
        yyyy = int(m[:4])
        mm = int(m[4:])
        return (yyyy, mm)
    pat = r'^(\d{4})년\s*(\d{1,2})월$'
    mmatch = re.match(pat, m)
    if mmatch:
        yyyy = int(mmatch.group(1))
        mm = int(mmatch.group(2))
        return (yyyy, mm)
    return m

def almost_equal(a, b, tol=1e-3):
    return abs(a - b) < tol

def get_next_month_str(ym: str) -> str:
    year = int(ym[:4])
    month = int(ym[4:])
    month += 1
    if month > 12:
        year += 1
        month = 1
    return f"{year}{month:02d}"

def get_prev_month_str(ym: str) -> str:
    """
    'YYYYMM' → 바로 직전 달 'YYYYMM'
    예) 202501 → 202412
    """
    year = int(ym[:4])
    month = int(ym[4:])
    month -= 1
    if month < 1:
        year -= 1
        month = 12
    return f"{year}{month:02d}"

def create_new_spreadsheet(filename: str, folder_id: str, drive_svc, attempt=1, max_attempts=5) -> str:
    try:
        query = (
            f"parents in '{folder_id}' and trashed=false "
            f"and name='{filename}'"
        )
        response = drive_svc.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=50
        ).execute()
        files = response.get("files", [])
        if files:
            existing_file_id = files[0]["id"]
            print(f"파일 '{filename}' 이미 존재 -> 재사용 (ID={existing_file_id})")
            return existing_file_id

        file_metadata = {
            "name": filename,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        }
        file = drive_svc.files().create(body=file_metadata).execute()
        return file["id"]

    except HttpError as e:
        if (e.resp.status == 403 and
            "userRateLimitExceeded" in str(e) and
            attempt < max_attempts):
            sleep_sec = 2 ** attempt
            print(f"[WARN] userRateLimitExceeded -> {sleep_sec}초 후 재시도 ({attempt}/{max_attempts})")
            time.sleep(sleep_sec)
            return create_new_spreadsheet(filename, folder_id, drive_svc, attempt+1, max_attempts)
        else:
            raise e

def batch_add_sheets(spreadsheet_id, sheet_svc, list_of_sheet_titles):
    meta = sheet_svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing_sheets = meta["sheets"]
    existing_titles = [s["properties"]["title"] for s in existing_sheets]

    missing = [t for t in list_of_sheet_titles if t not in existing_titles]
    if not missing:
        print("모든 시트가 이미 존재합니다.")
        return

    BATCH_SIZE = 30
    requests_add = []
    total_count = 0
    
    for title in missing:
        requests_add.append({
            "addSheet": {
                "properties": {
                    "title": title,
                    "gridProperties": {
                        "rowCount": 200,
                        "columnCount": 8
                    }
                }
            }
        })

        if len(requests_add) >= BATCH_SIZE:
            body = {"requests": requests_add}
            resp = sheet_svc.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()

            total_count += len(resp["replies"])
            print(f"분할 addSheet 완료: {len(resp['replies'])}개 생성")
            requests_add.clear()
            time.sleep(2)

    if requests_add:
        body = {"requests": requests_add}
        resp = sheet_svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        total_count += len(resp["replies"])
        print(f"마지막 addSheet 완료: {len(resp['replies'])}개 생성")
        requests_add.clear()

    print(f"시트 생성 총 개수: {total_count}")
    for idx, rep in enumerate(resp["replies"]):
        sheet_props = rep["addSheet"]["properties"]
        print(f" -> {idx} '{sheet_props['title']}' (sheetId={sheet_props['sheetId']})")

def duplicate_worksheet_with_new_name(gs_obj, from_sheet_name: str, to_sheet_name: str):
    all_ws = gs_obj.worksheets()
    all_titles = [w.title for w in all_ws]
    from_ws = None
    for w in all_ws:
        if w.title == from_sheet_name:
            from_ws = w
            break
    if not from_ws:
        raise ValueError(f"원본 시트 '{from_sheet_name}'를 찾을 수 없습니다.")

    base_name = to_sheet_name
    idx = 2
    while to_sheet_name in all_titles:
        to_sheet_name = f"{base_name} ({idx})"
        idx += 1

    new_ws = gs_obj.duplicate_sheet(source_sheet_id=from_ws.id, new_sheet_name=to_sheet_name)
    return new_ws

def is_korean_char(ch: str):
    return "가" <= ch <= "힣"

def is_korean_string(s: str):
    return any(is_korean_char(ch) for ch in s)

def album_sort_key(album_name: str):
    return (0 if is_korean_string(album_name) else 1, album_name)

def to_currency(num):
    return f"₩{format(int(round(num)), ',')}"

def update_next_month_tab(song_cost_sh, ym: str):
    """
    예시 함수 (기존 코드 내 사용)
    """
    old_ws = song_cost_sh.worksheet(ym)
    old_data = old_ws.get_all_values()
    if not old_data:
        print(f"'{ym}' 탭이 비어 있음")
        return

    old_header = old_data[0]
    old_body   = old_data[1:]

    try:
        idx_artist_old = old_header.index("아티스트명")
        idx_remain_old = old_header.index("당월 잔액")
    except ValueError:
        print("이전 달 시트에 '아티스트명' 또는 '당월 잔액' 칼럼이 없습니다.")
        return

    # 전월 잔액을 dict로 모아둠
    prev_month_dict = {}
    for row in old_body:
        artist_name = row[idx_artist_old].strip()
        if not artist_name or artist_name in ("합계","총계"):
            continue
        try:
            remain_val = float(row[idx_remain_old].replace(",", ""))
        except:
            remain_val = 0.0
        prev_month_dict[artist_name] = remain_val

    # 다음 달 시트 만들기(복제)
    next_ym = get_next_month_str(ym)
    new_ws = duplicate_worksheet_with_new_name(song_cost_sh, ym, next_ym)
    
    # 복제된 시트의 데이터 읽기
    new_data = new_ws.get_all_values()
    if not new_data:
        print(f"복제된 '{next_ym}' 탭이 비어 있습니다.")
        return

    new_header = new_data[0]
    try:
        idx_artist_new = new_header.index("아티스트명")
        idx_prev_new   = new_header.index("전월 잔액")
        idx_deduct_new = new_header.index("당월 차감액")
        # idx_remain_new = new_header.index("당월 잔액")
    except ValueError:
        print("새로 만든 시트(다음 달 탭)에 필요한 칼럼이 없습니다.")
        return
    
    # 본문 (마지막 합계 행은 제외)
    content = new_data[1:-1]

    updated_prev_vals = []   # D열에 들어갈 값
    updated_deduct_vals = [] # F열에 들어갈 값

    for row in content:
        artist = row[idx_artist_new].strip()
        old_val = prev_month_dict.get(artist, 0.0)  # 전월 잔액
        updated_prev_vals.append([old_val])
        updated_deduct_vals.append(["0"])  # 당월 차감액은 0으로 초기화

    row_count = len(content)
    start_row = 2
    end_row   = 1 + row_count

    # batch_update에 쓸 requests
    requests_body = [
        {
            "range": f"D{start_row}:D{end_row}",
            "values": updated_prev_vals
        },
        {
            "range": f"F{start_row}:F{end_row}",
            "values": updated_deduct_vals
        }
    ]

    # 한 번에 batch_update로 호출
    new_ws.batch_update(
        requests_body,
        value_input_option="USER_ENTERED"
    )

    print(f"'{ym}' → '{next_ym}' 탭 복제 및 전월/당월 차감액만 갱신(배치 업데이트) 완료!")



# ------------------------------------------------------------------------------
# (A) "0) 곡비 파일 수정" 섹션
# ------------------------------------------------------------------------------
def section_zero_prepare_song_cost():
    """
    (수정)
    - 이번 달(YYYYMM)과 직전 달(YYYYMM) 탭을 열어, '전월 잔액 + 당월 발생액'과
      실제 매출(UMAG/FLUXUS)을 비교하여 '당월 차감액' 갱신
    - Fluxus 소속 아티스트는 (fluxus_song + fluxus_yt) 인풋파일 매출액을 합산하여 사용
    - '2개 소속'인 경우는 스킵(차감액=0 or 공백)
    """
    st.subheader("0) 곡비 파일 수정")

    default_ym = st.session_state.get("ym", "")
    new_ym = st.text_input("진행기간(YYYYMM) - (곡비 파일 수정용)", default_ym)

    if st.button("곡비 파일 수정하기"):
        creds_a = get_credentials_from_secrets("A")
        gc_a = gspread.authorize(creds_a)

        if not re.match(r'^\d{6}$', new_ym):
            st.error("진행기간은 YYYYMM 6자리로 입력해야 합니다.")
            return
        st.session_state["ym"] = new_ym

        prev_ym = get_prev_month_str(new_ym)

        # (1) input_song cost 열기
        try:
            song_cost_sh = gc_a.open("input_song cost")
        except gspread.exceptions.SpreadsheetNotFound:
            st.error("Google Sheet 'input_song cost'를 찾을 수 없습니다.")
            return

        # (2) input_online revenue_umag_Integrated 열기
        try:
            umag_sh = gc_a.open("input_online revenue_umag_Integrated")
        except gspread.exceptions.SpreadsheetNotFound:
            st.error("Google Sheet 'input_online revenue_umag_Integrated'를 찾을 수 없습니다.")
            return
        
        # (3) fluxus 인풋파일 2개 열기
        try:
            fluxus_song_sh = gc_a.open("input_online revenue_fluxus_song")
        except gspread.exceptions.SpreadsheetNotFound:
            st.error("Google Sheet 'input_online revenue_fluxus_song'를 찾을 수 없습니다.")
            return

        try:
            fluxus_yt_sh = gc_a.open("input_online revenue_fluxus_yt")
        except gspread.exceptions.SpreadsheetNotFound:
            st.error("Google Sheet 'input_online revenue_fluxus_yt'를 찾을 수 없습니다.")
            return

        # ---------------------------
        # 0-A) 직전 달 "당월 잔액" dict
        # ---------------------------
        ws_map_sc = {ws.title: ws for ws in song_cost_sh.worksheets()}
        if prev_ym not in ws_map_sc:
            st.error(f"'input_song cost'에 직전 달 '{prev_ym}' 탭이 없습니다.")
            return
        ws_prev = ws_map_sc[prev_ym]
        data_prev = ws_prev.get_all_values()
        if not data_prev:
            st.error(f"'{prev_ym}' 탭이 비어있습니다.")
            return

        header_prev = data_prev[0]
        body_prev = data_prev[1:]
        try:
            idx_artist_p = header_prev.index("아티스트명")
            idx_remain_p = header_prev.index("당월 잔액")
        except ValueError as e:
            st.error(f"직전 달 '{prev_ym}' 시트에 '아티스트명' 또는 '당월 잔액' 칼럼이 없습니다: {e}")
            return

        prev_remain_dict = {}
        for row_p in body_prev:
            artist = clean_artist_name(row_p[idx_artist_p])
            if not artist or artist in ("합계", "총계"):
                continue
            try:
                val = float(row_p[idx_remain_p].replace(",", ""))
            except:
                val = 0.0
            prev_remain_dict[artist] = val

        # ---------------------------
        # 0-B) 이번 달 탭(소속, 전월잔액, 당월 발생액, 당월 차감액...)
        # ---------------------------
        if new_ym not in ws_map_sc:
            st.error(f"이번 달 '{new_ym}' 탭이 없습니다.")
            return
        
        ws_new = ws_map_sc[new_ym]
        data_new = ws_new.get_all_values()
        if not data_new:
            st.error(f"'{new_ym}' 탭이 비어있습니다.")
            return

        header_new = data_new[0]
        body_new = data_new[1:-1]  # 마지막 합계 행 제외

        # 필수 칼럼
        try:
            idx_sosok_n  = header_new.index("소속")  # <--- 새로 추가(가정): "UMAG"/"FLUXUS" 중 하나
            idx_artist_n = header_new.index("아티스트명")
            idx_prev_n   = header_new.index("전월 잔액")
            idx_curr_n   = header_new.index("당월 발생액")
            idx_ded_n    = header_new.index("당월 차감액")
        except ValueError as e:
            st.error(f"[input_song cost-{new_ym}]에 '소속' 칼럼이 없거나 필요한 칼럼이 없습니다: {e}")
            return

        # ---------------------------
        # 0-C) UMAG 매출 dict
        # ---------------------------
        ws_map_umag = {ws.title: ws for ws in umag_sh.worksheets()}
        if new_ym not in ws_map_umag:
            st.error(f"'input_online revenue_umag_Integrated'에 '{new_ym}' 탭이 없습니다.")
            return
        ws_umag = ws_map_umag[new_ym]
        data_umag = ws_umag.get_all_values()
        header_umag = data_umag[0]
        body_umag   = data_umag[1:]

        try:
            col_artist_umag = header_umag.index("앨범아티스트")
            col_revenue_umag= header_umag.index("권리사정산금액")
        except ValueError:
            st.error("umag 인풋파일에서 '앨범아티스트' 또는 '권리사정산금액' 칼럼을 찾을 수 없습니다.")
            return
        
        sum_umag_dict = defaultdict(float)
        for row_u in body_umag:
            a = clean_artist_name(row_u[col_artist_umag])
            if not a:
                continue
            try:
                val = float(row_u[col_revenue_umag].replace(",", ""))
            except:
                val = 0.0
            sum_umag_dict[a] += val

        # ---------------------------
        # 0-D) FLUXUS(song) 매출 dict
        # ---------------------------
        ws_map_flux_song = {ws.title: ws for ws in fluxus_song_sh.worksheets()}
        if new_ym not in ws_map_flux_song:
            st.error(f"'input_online revenue_fluxus_song'에 '{new_ym}' 탭이 없습니다.")
            return
        ws_flux_song = ws_map_flux_song[new_ym]
        data_flux_song = ws_flux_song.get_all_values()
        header_fs = data_flux_song[0]
        body_fs   = data_flux_song[1:]

        try:
            col_artist_fs = header_fs.index("가수명")  # 기존 앨범아티스트
            col_revenue_fs= header_fs.index("권리사 정산액")
        except ValueError:
            st.error("fluxus_song 인풋파일에 '가수명' 또는 '권리사 정산액' 칼럼이 없습니다.")
            return

        sum_flux_song_dict = defaultdict(float)
        for row_fs in body_fs:
            a = clean_artist_name(row_fs[col_artist_fs])
            if not a:
                continue
            try:
                val = float(row_fs[col_revenue_fs].replace(",", ""))
            except:
                val = 0.0
            sum_flux_song_dict[a] += val

        # ---------------------------
        # 0-E) FLUXUS(yt) 매출 dict
        # ---------------------------
        ws_map_flux_yt = {ws.title: ws for ws in fluxus_yt_sh.worksheets()}
        if new_ym not in ws_map_flux_yt:
            st.error(f"'input_online revenue_fluxus_yt'에 '{new_ym}' 탭이 없습니다.")
            return
        ws_flux_yt = ws_map_flux_yt[new_ym]
        data_flux_yt = ws_flux_yt.get_all_values()
        header_fy = data_flux_yt[0]
        body_fy   = data_flux_yt[1:]

        try:
            col_artist_fy = header_fy.index("ALBIM ARTIST")  # 기존 '앨범아티스트'
            col_revenue_fy= header_fy.index("권리사 정산액 \n(KRW)")
        except ValueError:
            st.error("fluxus_yt 인풋파일에 'ALBIM ARTIST' 또는 '권리사 정산액 \\n(KRW)' 칼럼이 없습니다.")
            return

        sum_flux_yt_dict = defaultdict(float)
        for row_fy in body_fy:
            a = clean_artist_name(row_fy[col_artist_fy])
            if not a:
                continue
            try:
                val = float(row_fy[col_revenue_fy].replace(",", ""))
            except:
                val = 0.0
            sum_flux_yt_dict[a] += val

        # ---------------------------------------
        # 0-F) batch_update 준비
        # ---------------------------------------
        updated_vals_for_def = []
        double_sosok_artists = []  # 2개 소속 중복된 아티스트 기록

        for row_idx, row_data in enumerate(body_new):
            artist_n = clean_artist_name(row_data[idx_artist_n])
            sosok_n  = row_data[idx_sosok_n].strip().upper()  # 예: "UMAG" / "FLUXUS" / ...
            if not artist_n or artist_n in ("합계","총계"):
                updated_vals_for_def.append(["","",""])  # 전월, 당월발생, 당월차감 공란
                continue

            # 전월 잔액
            prev_val = prev_remain_dict.get(artist_n, 0.0)

            # 당월 발생액 (시트에 이미 기재된 값)
            try:
                curr_val_str = row_data[idx_curr_n].replace(",","")
                curr_val = float(curr_val_str) if curr_val_str else 0.0
            except:
                curr_val = 0.0

            # '소속' 검사
            #  - UMAG or FLUXUS 중 1개만이어야 함
            #  - 2개이상 or 알수없는 값 => 스킵
            splitted = re.split(r'[,&/]', sosok_n)  # 예: "UMAG,FLUXUS" 등
            splitted = [x.strip() for x in splitted if x.strip()]
            if len(splitted) > 1:
                # 2개 이상 소속
                double_sosok_artists.append(artist_n)
                updated_vals_for_def.append([prev_val, curr_val, ""])
                continue

            # 매출합
            if sosok_n == "UMAG":
                total_revenue = sum_umag_dict.get(artist_n, 0.0)
            elif sosok_n == "FLUXUS":
                # fluxus_song + fluxus_yt
                fs_val = sum_flux_song_dict.get(artist_n, 0.0)
                fy_val = sum_flux_yt_dict.get(artist_n, 0.0)
                total_revenue = fs_val + fy_val
            else:
                # 알수없는 소속이거나 공란 => 스킵
                updated_vals_for_def.append([prev_val, curr_val, ""])
                continue

            can_deduct = prev_val + curr_val
            actual_deduct = min(total_revenue, can_deduct)

            updated_vals_for_def.append([prev_val, curr_val, actual_deduct])

        # batch_update
        total_rows = len(body_new)
        start_row = 2
        end_row = 1 + total_rows
        range_notation = f"E{start_row}:G{end_row}"
        requests_body = [
            {
                "range": range_notation,
                "values": updated_vals_for_def
            }
        ]
        ws_new.batch_update(requests_body, value_input_option="USER_ENTERED")

        # 업데이트 완료 후, 2개 소속(중복) 아티스트 안내
        if double_sosok_artists:
            msg = f"2개 소속이 중복되어 작업에서 제외된 아티스트: {double_sosok_artists}"
            st.warning(msg)
 
        # 디버깅용: 모든 아티스트 출력
        for i, row_fs in enumerate(body_fs):
            raw = row_fs[col_artist_fs]
            debugged = debug_hex(raw)
            st.write(f"[fluxus_song] row={i}, raw='{raw}', hex={debugged}")

            a = clean_artist_name(raw)
            if not a:
                continue

        st.success(f"곡비 파일('{new_ym}' 탭) 수정 완료!")
        st.session_state["song_cost_prepared"] = True
        # double_sosok_artists 내역을 session_state 등에 저장해도 됨
        st.session_state["excluded_double_sosok"] = double_sosok_artists





# ------------------------------------------------------------------------------
# (B) "1) 정산 보고서 정보 입력 항목" 섹션
# ------------------------------------------------------------------------------
def section_one_report_input():
    st.subheader("1) 정산 보고서 정보 입력 항목")

    default_ym = st.session_state.get("ym", "")
    default_report_date = st.session_state.get("report_date", "")

    ym = st.text_input("진행기간(YYYYMM)", default_ym)
    report_date = st.text_input("보고서 발행 날짜 (YYYY-MM-DD)", default_report_date)

    # 진행 상황 표시용
    progress_bar = st.empty()
    artist_placeholder = st.empty()

    if st.button("정산 보고서 생성 시작"):
        if not re.match(r'^\d{6}$', ym):
            st.error("진행기간은 YYYYMM 6자리로 입력하세요.")
            return
        if not report_date:
            st.error("보고서 발행 날짜를 입력하세요.")
            return

        st.session_state["ym"] = ym
        st.session_state["report_date"] = report_date

        creds_a = get_credentials_from_secrets("A")
        gc_a = gspread.authorize(creds_a)
        drive_svc_a = build("drive", "v3", credentials=creds_a)
        sheet_svc_a = build("sheets", "v4", credentials=creds_a)

        check_dict = {
            "song_artists": [],
            "revenue_artists": [],
            "artist_compare_result": {}
        }

        out_file_id = generate_report(
            ym, report_date, check_dict,
            gc=gc_a,
            drive_svc=drive_svc_a,
            sheet_svc=sheet_svc_a,
            progress_bar=progress_bar,
            artist_placeholder=artist_placeholder
        )

        st.session_state["report_done"] = True
        st.session_state["report_file_id"] = out_file_id
        st.session_state["check_dict"] = check_dict

        st.success(f"보고서 생성 완료! file_id={out_file_id}")
        if out_file_id:
            artist_placeholder.success("모든 아티스트 정산 보고서 생성 완료!")
            time.sleep(1)
            artist_placeholder.empty()
            progress_bar.empty()


# ------------------------------------------------------------------------------
# (C) 보고서 링크 & 검증
# ------------------------------------------------------------------------------
def section_two_sheet_link_and_verification():
    if "report_done" in st.session_state and st.session_state["report_done"]:
        st.subheader("2) 정산 보고서 시트링크 및 검증")

        tab1, tab2 = st.tabs(["보고서 링크 / 요약", "세부 검증 내용"])

        with tab1:
            out_file_id = st.session_state.get("report_file_id", "")
            if out_file_id:
                gsheet_url = f"https://docs.google.com/spreadsheets/d/{out_file_id}/edit"
                st.write(f"**생성된 구글시트 링크:** {gsheet_url}")

            cd = st.session_state.get("check_dict", {})
            if cd:
                # 여기에 필요한 검증 결과 요약 표시
                st.write("검증 결과 요약:")
                st.write(cd.get("artist_compare_result"))
            else:
                st.info("검증 데이터 없음")

            # 2개 소속 중복된 아티스트 안내
            double_sosok_list = st.session_state.get("excluded_double_sosok", [])
            if double_sosok_list:
                st.warning(f"2개 소속 중복으로 작업 제외된 아티스트: {double_sosok_list}")

        with tab2:
            st.write("### 세부 검증 내용")
            st.info("인풋데이터 vs 산출결과 비교")
            # show_detailed_verification() 같은 함수 호출

    else:
        st.info("정산 보고서 생성 완료 후 확인 가능합니다.")


# ------------------------------------------------------------------------------
# (D) 엑셀 업로드 → 아티스트별 XLSX 파일 분할
# ------------------------------------------------------------------------------
def section_three_upload_and_split_excel():
    """
    (수정 버전)
    1) 사용자가 "output_report_YYYYMM.xlsx" (구글시트에서 다운로드)를 업로드
    2) 각 '아티스트(정산서)' 탭 + '아티스트(세부매출내역)' 탭을 묶어서
       → '소속_정산보고서_아티스트명_YYYYMM.xlsx' 로 저장
    3) 모든 아티스트 파일을 하나의 ZIP으로 다운로드
    """
    if "report_done" not in st.session_state or not st.session_state["report_done"]:
        st.info("정산 보고서가 먼저 생성된 뒤에, 엑셀 업로드 가능합니다.")
        return

    st.subheader("3) 엑셀 업로드 후 [아티스트별] 엑셀파일로 분할 (서식 유지)")

    st.write("""
    **사용 순서**  
    1. 생성된 구글시트(보고서)에서 "**파일 → 다운로드 → Microsoft Excel (.xlsx)**"로 다운로드  
    2. 아래 업로드 버튼으로 방금 받은 .xlsx 파일을 업로드  
    3. 아티스트별로 '정산서' 탭 + '세부매출내역' 탭만 묶은 XLSX를 각각 만들고, ZIP으로 묶어 다운로드
    """)

    uploaded_file = st.file_uploader("엑셀 파일 업로드", type=["xlsx"])
    if uploaded_file is None:
        return

    progress_bar = st.progress(0.0)
    progress_text = st.empty()

    # A) 전체 엑셀 로드
    original_file_data = uploaded_file.read()
    try:
        wb_all = openpyxl.load_workbook(io.BytesIO(original_file_data))
    except Exception as e:
        st.error(f"엑셀 파일을 읽는 중 오류 발생: {e}")
        return

    sheet_names = wb_all.sheetnames

    # B) '아티스트(정산서)', '아티스트(세부매출내역)' 페어를 찾아서 처리
    #    예) "홍길동(정산서)", "홍길동(세부매출내역)"
    #    아티스트명 추출: 탭이름.split("(")[0] (단, 주의)
    #    소속: 탭 안쪽 데이터 1행 or input_song cost에서?

    # 여기서는 탭 이름 형식 "[아티스트명](정산서)" vs "[아티스트명](세부매출내역)" 만 있다고 가정.
    # 실제로는 '소속'을 알기 위해서, "input_song cost"를 다시 참조하거나
    # 또는 보고서 시트 내부에 '소속' 정보를 감춰둔 셀에서 읽어올 수도 있습니다.
    # 간단히는, 탭 데이터 2~3행에 소속 표기했거나, 세션에 저장했을 수도.
    # 여기서는 예시로, 탭 이름 내 "UMAG_" or "FLUXUS_" 같은 식으로 만들었다고 가정하셔도 됩니다.

    # 본 예시에서는 "소속" 정보를 알 수 없다면, 일단 "UNKNOWN"으로 처리
    # 실제 구현 시에는 generate_report할 때 탭 이름에 "[소속]아티스트명(정산서)" 식으로 지어주면 더 간단.
    # 여기서는 "input_song cost"를 다시 열어 mapping table을 구성한다고 가정합니다.

    # 우선 아티스트 목록
    all_pairs = defaultdict(lambda: {"report": None, "detail": None})

    for sn in sheet_names:
        if sn.endswith("(정산서)"):
            artist_name = sn[:-5]  # '(정산서)' 제거
            artist_name = artist_name.strip()
            all_pairs[artist_name]["report"] = sn
        elif sn.endswith("(세부매출내역)"):
            artist_name = sn[:-7]  # '(세부매출내역)' 제거
            artist_name = artist_name.strip()
            all_pairs[artist_name]["detail"] = sn
        else:
            pass  # 무시(합계시트 등 다른 시트가 있을 수 있음)

    # (옵션) 소속 정보를 가져오기 위해 다시 "input_song cost" 열어서, ym탭에서 dict 생성
    #        artist_sosok_dict[아티스트명] = "UMAG" or "FLUXUS" ...
    artist_sosok_dict = {}
    try:
        creds_a = get_credentials_from_secrets("A")
        gc_a = gspread.authorize(creds_a)
        sc_sh = gc_a.open("input_song cost")
        sc_ws_map = {ws.title: ws for ws in sc_sh.worksheets()}
        current_ym = st.session_state.get("ym", "")
        if current_ym in sc_ws_map:
            ws_sc = sc_ws_map[current_ym]
            data_sc = ws_sc.get_all_values()
            if data_sc:
                hdr_sc = data_sc[0]
                try:
                    idx_a = hdr_sc.index("아티스트명")
                    idx_s = hdr_sc.index("소속")
                except:
                    idx_a, idx_s = -1, -1
                for row_sc in data_sc[1:]:
                    a = row_sc[idx_a].strip() if idx_a>=0 else ""
                    s = row_sc[idx_s].strip().upper() if idx_s>=0 else ""
                    if a and s:
                        # 혹시 중복 기재라면?
                        # 일단 마지막 값으로 덮어씀
                        # (실제로는 더 정교한 로직 가능)
                        artist_sosok_dict[a] = s
        else:
            pass
    except:
        pass

    # C) ZIP 버퍼 준비
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        total_pairs = len(all_pairs)
        for i, (artist, pair_info) in enumerate(all_pairs.items()):
            ratio = (i+1)/total_pairs
            progress_bar.progress(ratio)
            progress_text.info(f"{int(ratio*100)}% - '{artist}' 처리 중...")

            ws_report_name = pair_info["report"]
            ws_detail_name = pair_info["detail"]
            if not ws_report_name or not ws_detail_name:
                # 한쪽 탭만 존재하는 경우는 스킵
                continue

            # 소속
            sosok = artist_sosok_dict.get(artist, "UNKNOWN")

            # (1) 새 워크북 생성
            temp_wb = openpyxl.Workbook()
            # 기본 생성 시트 제거
            def_ws = temp_wb.active
            temp_wb.remove(def_ws)

            # (2) 원본에서 "정산서" 탭 복사
            orig_ws_report = wb_all[ws_report_name]
            new_ws_report = temp_wb.create_sheet(ws_report_name)
            copy_sheet(orig_ws_report, new_ws_report)

            # (3) 원본에서 "세부매출내역" 탭 복사
            orig_ws_detail = wb_all[ws_detail_name]
            new_ws_detail = temp_wb.create_sheet(ws_detail_name)
            copy_sheet(orig_ws_detail, new_ws_detail)

            # (4) 파일명: f"{소속}_정산보고서_{artist}_{current_ym}.xlsx"
            safe_artist = artist.replace("/", "_").replace("\\", "_")
            filename_xlsx = f"{sosok}_정산보고서_{safe_artist}_{current_ym}.xlsx"

            single_buf = io.BytesIO()
            temp_wb.save(single_buf)
            single_buf.seek(0)

            zf.writestr(filename_xlsx, single_buf.getvalue())

    zip_buf.seek(0)
    progress_text.success("아티스트별 엑셀 생성 완료!")
    st.download_button(
        label="ZIP 다운로드",
        data=zip_buf.getvalue(),
        file_name="report_by_artist.zip",
        mime="application/zip"
    )


def copy_sheet(src_ws, dst_ws):
    """
    src_ws(Openpyxl worksheet) → dst_ws(Openpyxl worksheet)에 셀 값/서식 등 복사
    - openpyxl은 완벽한 서식 복제가 쉽지 않으므로, 여기서는 가장 기본적인 값/스타일만 복사
    - 필요시 엑셀 스타일 속성(border/fill/font/alignment 등)을 더 옮겨줄 수 있음
    """
    from copy import copy
    max_row = src_ws.max_row
    max_col = src_ws.max_column

    for r in range(1, max_row+1):
        dst_ws.row_dimensions[r].height = src_ws.row_dimensions[r].height
        for c in range(1, max_col+1):
            cell_src = src_ws.cell(row=r, column=c)
            cell_dst = dst_ws.cell(row=r, column=c, value=cell_src.value)
            if cell_src.has_style:
                cell_dst.font = copy(cell_src.font)
                cell_dst.border = copy(cell_src.border)
                cell_dst.fill = copy(cell_src.fill)
                cell_dst.number_format = copy(cell_src.number_format)
                cell_dst.protection = copy(cell_src.protection)
                cell_dst.alignment = copy(cell_src.alignment)
    for c in range(1, max_col+1):
        dst_ws.column_dimensions[openpyxl.utils.get_column_letter(c)].width = src_ws.column_dimensions[openpyxl.utils.get_column_letter(c)].width


# ##############################################################################
# (E) 보고서 생성 함수(generate_report) - UMAG/FLUXUS 분기 포함
# ##############################################################################
def generate_report(
    ym: str,
    report_date: str,
    check_dict: dict,
    gc: gspread.Client,
    drive_svc,
    sheet_svc,
    progress_bar,
    artist_placeholder
):
    """
    (간략화 버전)
    1) input_song cost에서 아티스트 & 소속(U/F) & 곡비정보 읽기
    2) UMAG인 경우 기존 input_online revenue_umag_Integrated, FLUXUS인 경우 fluxus_song+yt
       → 아티스트별 매출정보를 읽어와 '세부매출' 구조화
    3) output_report_YYYYMM 시트를 생성하여, 아티스트별 (정산서, 세부매출내역) 탭 생성
      - FLUXUS인 경우 컬럼 구성 달리하기
    4) 2개 소속 중복아티스트는 스킵
    5) 처리 후 spreadsheetId 반환
    """
    folder_id = st.secrets["google_service_account_a"]["folder_id"]

    # 1) input_song cost
    try:
        sc_sh = gc.open("input_song cost")
    except gspread.exceptions.SpreadsheetNotFound:
        st.error("'input_song cost' 없음")
        return ""

    ws_map_sc = {ws.title: ws for ws in sc_sh.worksheets()}
    if ym not in ws_map_sc:
        st.error(f"input_song cost에 '{ym}' 탭이 없습니다.")
        return ""
    ws_sc = ws_map_sc[ym]
    data_sc = ws_sc.get_all_values()
    if len(data_sc)<2:
        st.error(f"[{ym}] 탭 데이터 없음")
        return ""
    hdr_sc = data_sc[0]
    body_sc= data_sc[1:]

    # 소속, 곡비 등 인덱스
    try:
        idx_artist = hdr_sc.index("아티스트명")
        idx_sosok  = hdr_sc.index("소속")
        idx_prev   = hdr_sc.index("전월 잔액")
        idx_curr   = hdr_sc.index("당월 발생액")
        idx_deduct = hdr_sc.index("당월 차감액")
        idx_remain = hdr_sc.index("당월 잔액")
        idx_rate   = hdr_sc.index("정산 요율")
    except:
        st.error("곡비시트에 필요한 칼럼(소속, 전월 잔액 등)이 없습니다.")
        return ""

    # artist_cost_dict: { artist: {sosok, 전월잔액, 당월발생, 당월차감, 당월잔액, 정산요율} }
    artist_cost_dict = {}
    double_sosok_list = []
    for row in body_sc:
        a = row[idx_artist].strip()
        a = clean_artist_name(row[idx_artist])
        if not a or a in ("합계","총계"):
            continue
        s = row[idx_sosok].strip().upper()
        splitted = re.split(r'[,&/]', s)
        splitted = [x.strip() for x in splitted if x.strip()]
        if len(splitted)>1:
            double_sosok_list.append(a)
            continue
        # 숫자 변환
        def num(x):
            if not x: return 0.0
            return float(x.replace(",","").replace("%",""))
        artist_cost_dict[a] = {
            "sosok": s,
            "prev": num(row[idx_prev]),
            "curr": num(row[idx_curr]),
            "deduct": num(row[idx_deduct]),
            "remain": num(row[idx_remain]),
            "rate": num(row[idx_rate])
        }

    # (옵션) double 소속 안내
    if double_sosok_list:
        st.warning(f"2개 소속(중복) 아티스트: {double_sosok_list}")

    # 2) 각 인풋파일에서 매출 읽기 (UMAG vs FLUXUS)
    # ... (생략) => 여기서는 "세부매출" dict 가 있다고 가정
    # 실제론 곡비 섹션과 동일하게 umag_Integrated / fluxus_song / fluxus_yt 열어서 ym탭 읽고, 아티스트별로 수집

    # 임시로 "artist_revenue_dict = {아티스트: [ {album, revenue, major, middle, service, trackNo, trackTitle,...}, ... ]}"
    artist_revenue_dict = defaultdict(list)
    # 실제 구현시: artist_sosok_dict[a]=="UMAG" 면 umag_Integrated, "FLUXUS" 면 fluxus 2개 파일에서 수집

    # 3) output_report_YYYYMM 생성
    out_filename = f"output_report_{ym}"
    out_file_id = create_new_spreadsheet(out_filename, folder_id, drive_svc)
    out_sh = gc.open_by_key(out_file_id)

    # 기본 "Sheet1" 삭제
    try:
        out_sh.del_worksheet(out_sh.worksheet("Sheet1"))
    except:
        pass

    # (UI) 진행 표시
    all_artists = sorted(artist_cost_dict.keys())
    progress_bar.progress(0.0)
    artist_placeholder.info("생성 중...")

    # 시트 미리 생성
    needed_titles = []
    for a in all_artists:
        # double소속은 제외
        if a in double_sosok_list:
            continue
        needed_titles.append(f"{a}(세부매출내역)")
        needed_titles.append(f"{a}(정산서)")
    batch_add_sheets(out_file_id, sheet_svc, needed_titles)

    # 아티스트별 시트 채우기
    # (FLUXUS vs UMAG) 에 따라 컬럼 구성 다르게
    requests_batch = []
    for i, artist in enumerate(all_artists):
        ratio = (i+1)/len(all_artists)
        progress_bar.progress(ratio)
        artist_placeholder.info(f"[{i+1}/{len(all_artists)}] {artist} 처리 중...")

        if artist in double_sosok_list:
            continue

        # 소속
        sosok = artist_cost_dict[artist]["sosok"]
        if sosok!="UMAG" and sosok!="FLUXUS":
            # 알수없는 소속 => 스킵
            continue

        # 세부매출
        detail_rows = artist_revenue_dict[artist]  # 실제로는 2개 파일에서 합친 결과
        # 정렬
        detail_rows_sorted = sorted(detail_rows, key=lambda x: x.get("album",""))

        # 3-A) "세부매출내역" 탭
        ws_name_detail = f"{artist}(세부매출내역)"
        ws_detail = out_sh.worksheet(ws_name_detail)
        sheet_id_detail = ws_detail.id

        if sosok=="UMAG":
            # 기존 방식: [앨범아티스트, 앨범명, 대분류, 중분류, 서비스명, 기간, 매출순수익]
            matrix_detail = []
            matrix_detail.append(["앨범아티스트","앨범명","대분류","중분류","서비스명","기간","매출 순수익"])
            total_sum = 0
            for d in detail_rows_sorted:
                # major, middle, service
                rv = d.get("revenue",0)
                total_sum += rv
                matrix_detail.append([
                    artist,
                    d.get("album",""),
                    d.get("major",""),
                    d.get("middle",""),
                    d.get("service",""),
                    f"{ym[:4]}년 {ym[4:]}월",
                    f"{rv:,.0f}"
                ])
            matrix_detail.append(["합계","","","","","", f"{total_sum:,.0f}"])

        else:
            # FLUXUS 형식: [앨범아티스트, 앨범명, '트랙 No.', '트랙명', '매출 순수익']
            # 삭제: 대분류/중분류/서비스명
            # 추가: 트랙 No, 트랙명
            matrix_detail = []
            matrix_detail.append(["앨범아티스트","앨범명","트랙 No.","트랙명","매출 순수익"])
            total_sum = 0
            for d in detail_rows_sorted:
                rv = d.get("revenue",0)
                total_sum += rv
                matrix_detail.append([
                    artist,
                    d.get("album",""),
                    d.get("trackNo",""),
                    d.get("trackTitle",""),
                    f"{rv:,.0f}"
                ])
            matrix_detail.append(["합계","","","", f"{total_sum:,.0f}"])

        ws_detail.update("A1", matrix_detail)
        time.sleep(0.5)

        # (서식, 테두리 등) -> 생략 or requests_batch.append(...)
        # ...

        # 3-B) "정산서" 탭
        ws_name_report = f"{artist}(정산서)"
        ws_report = out_sh.worksheet(ws_name_report)
        sheet_id_report = ws_report.id

        # 곡비 = 전월+당월, 공제액 = ...
        # (UMAG vs FLUXUS) "음원 서비스별 정산내역" 칼럼 달라짐
        # ...
        # [생략 - 위와 유사]
        # ws_report.update("A1", some_matrix)
        # ...

        time.sleep(0.5)

    # batchUpdate all_requests (생략)
    # ...

    # 모두 완료
    artist_placeholder.success("모든 아티스트 처리 완료!")

    return out_file_id


# ------------------------------------------------------------------------------
# (F) 메인 구동
# ------------------------------------------------------------------------------
def main():
    st.title("아티스트 음원 정산 보고서 자동 생성기")

    # 0) 곡비파일 수정 섹션
    section_zero_prepare_song_cost()
    st.divider()

    # 1) 보고서 생성 섹션
    section_one_report_input()
    st.divider()

    # 2) 보고서 링크 & 검증
    section_two_sheet_link_and_verification()
    st.divider()

    # 3) 엑셀 업로드 -> 아티스트별 XLSX 분리
    section_three_upload_and_split_excel()


if __name__ == "__main__":
    main()
