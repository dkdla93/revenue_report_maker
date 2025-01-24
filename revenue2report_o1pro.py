import streamlit as st
import json
import datetime
import re
import time
import io
import zipfile
import requests as req

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

# (기존) collections
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
# (신규) [0] 맨 앞단에 "곡비 파일 수정" 섹션 추가
# ----------------------------------------------------------------

def section_zero_prepare_song_cost():
    """
    0) 곡비 파일 수정(제작) 섹션 (맨 앞단)
    - 이번 진행기간(YYYYMM) 입력받아,
    - input_online revenue의 해당 달 탭(매출),
      input_song cost의 직전 달 탭(당월잔액) & 이번 달 탭에 대해
      '당월 발생액'은 공란, '당월 차감액'은 min(매출합, 전월+당월발생액)으로 업데이트,
      '당월 잔액' 컬럼의 수식 유지 (직접 값 기입 X)
    """
    st.subheader("0) 곡비 파일 수정 (가장 먼저 실행)")

    default_ym = st.session_state.get("ym", "")
    new_ym = st.text_input("진행기간(YYYYMM) - (곡비 파일 수정용)", default_ym)

    if st.button("곡비 파일 수정하기"):
        # A 계정 인증
        creds_a = get_credentials_from_secrets("A")
        gc_a = gspread.authorize(creds_a)

        # 1) 유효성 체크
        if not re.match(r'^\d{6}$', new_ym):
            st.error("진행기간은 YYYYMM 6자리로 입력해야 합니다.")
            return

        # session_state에 저장 (아래 단계에서 재사용할 수 있도록)
        st.session_state["ym"] = new_ym

        # 2) 직전 달(YYYYMM) 계산
        prev_ym = get_prev_month_str(new_ym)  # 밑에서 정의할 헬퍼

        try:
            # (A) input_song cost 열기
            song_cost_sh = gc_a.open("input_song cost")
        except gspread.exceptions.SpreadsheetNotFound:
            st.error("Google Sheet 'input_song cost'를 찾을 수 없습니다.")
            return

        try:
            # (B) input_online revenue 열기
            revenue_sh = gc_a.open("input_online revenue")
        except gspread.exceptions.SpreadsheetNotFound:
            st.error("Google Sheet 'input_online revenue'를 찾을 수 없습니다.")
            return

        # ---- (1) 직전달 탭에서 '아티스트별 당월 잔액' 읽기 ----
        ws_map_sc = {ws.title: ws for ws in song_cost_sh.worksheets()}
        if prev_ym not in ws_map_sc:
            st.error(f"input_song cost에 직전 달 '{prev_ym}' 탭이 없습니다.")
            return
        ws_prev = ws_map_sc[prev_ym]
        data_prev = ws_prev.get_all_values()
        if not data_prev:
            st.error(f"'{prev_ym}' 탭이 비어있습니다.")
            return

        header_prev = data_prev[0]
        body_prev   = data_prev[1:]
        try:
            idx_artist_p = header_prev.index("아티스트명")
            idx_remain_p = header_prev.index("당월 잔액")
        except ValueError as e:
            st.error(f"직전 달 시트에 '아티스트명' 또는 '당월 잔액' 칼럼이 없습니다: {e}")
            return

        prev_remain_dict = {}
        for row in body_prev:
            artist = row[idx_artist_p].strip()
            if not artist or artist in ("합계", "총계"):
                continue
            try:
                val = float(row[idx_remain_p].replace(",", ""))
            except:
                val = 0.0
            prev_remain_dict[artist] = val

        # ---- (2) 이번 달 탭에서 '아티스트명' / '정산 요율' / '전월 잔액' 등 읽기 ----
        if new_ym not in ws_map_sc:
            # 만약 이번 달 탭이 없으면, 직전 달 탭을 복제해서 만들 수도 있음
            # (요청 사항에 따라 처리. 여기서는 "없으면 에러" 예시, 필요시 duplicate_worksheet_with_new_name 활용)
            st.error(f"이번 달 '{new_ym}' 탭이 없어서 작업할 수 없습니다. (필요시 복제 로직 추가)")
            return

        ws_new = ws_map_sc[new_ym]
        data_new = ws_new.get_all_values()
        if not data_new:
            st.error(f"'{new_ym}' 탭이 비어있습니다.")
            return
        
        header_new = data_new[0]
        # 맨 마지막 합계행(합계,총계)이 수식/합계를 포함하는 경우 → body_new = data_new[1:-1]로 제외
        body_new = data_new[1:-1]  # [중요] 마지막 행(합계)은 업데이트에서 제외

        # 칼럼 인덱스 찾기
        try:
            idx_artist_n = header_new.index("아티스트명")
            idx_prev_n   = header_new.index("전월 잔액")  
            idx_curr_n   = header_new.index("당월 발생액")
            idx_ded_n    = header_new.index("당월 차감액")
            # idx_remain_n = header_new.index("당월 잔액")  # 수식 칼럼 → 안 건드림
        except ValueError as e:
            st.error(f"[input_song cost-{new_ym}] 시트 칼럼 확인 필요: {e}")
            return

        # ---- (3) 이번 달의 매출합 (input_online revenue) ----
        ws_map_or = {ws.title: ws for ws in revenue_sh.worksheets()}
        if new_ym not in ws_map_or:
            st.error(f"input_online revenue에 '{new_ym}' 탭이 없습니다.")
            return
        ws_rev = ws_map_or[new_ym]
        data_rev = ws_rev.get_all_values()
        if not data_rev:
            st.error(f"{new_ym} 탭(매출)이 비어있습니다.")
            return

        header_rev = data_rev[0]
        body_rev   = data_rev[1:]

        try:
            col_artist_rev = header_rev.index("앨범아티스트")
            col_revenue    = header_rev.index("권리사정산금액")
        except ValueError as e:
            st.error(f"[input_online revenue-{new_ym}] '앨범아티스트' 또는 '권리사정산금액' 칼럼이 없습니다: {e}")
            return

        # 아티스트별 매출 합산
        from collections import defaultdict
        sum_revenue_dict = defaultdict(float)
        for row in body_rev:
            a = row[col_artist_rev].strip()
            if not a: 
                continue
            try:
                rv_val = float(row[col_revenue].replace(",", ""))
            except:
                rv_val = 0.0
            sum_revenue_dict[a] += rv_val


        # -------------------------------------------
        # [중요] batch_update를 위한 2D 배열 만들기
        # -------------------------------------------
        total_rows = len(body_new)  # 합계행 제외한 개수
        # D열 ~ F열에 쓸 데이터(각각 row 수만큼 2D)
        #  - D열(index=3) → 전월 잔액
        #  - E열(index=4) → 당월 발생액
        #  - F열(index=5) → 당월 차감액
        updated_vals_for_def = []  # shape: [total_rows][3]

        for row_idx, row_data in enumerate(body_new):
            artist_n = row_data[idx_artist_n].strip()
            if not artist_n or artist_n in ("합계", "총계"):
                # 혹시 중간에 '합계'가 있으면 무시
                updated_vals_for_def.append(["", "", ""])
                continue

            old_prev_val = prev_remain_dict.get(artist_n, 0.0)
            curr_val = 0  # 당월발생액
            rev_sum = sum_revenue_dict.get(artist_n, 0.0)

            # min(전월+당월발생, 매출합)
            can_deduct = old_prev_val + curr_val
            actual_deduct = rev_sum if rev_sum <= can_deduct else can_deduct

            updated_vals_for_def.append([
                old_prev_val,
                curr_val,
                actual_deduct
            ])

        # 2) Range 설정: 
        #    - gspread에서 "A1표기"로 "D2:F{N+1}" 을 업데이트하면
        #      body_new의 row_idx=0 → 시트의 2행
        #      row_idx=(N-1) → 시트의 (N+1)행
        start_row = 2  # 시트상 2행부터 데이터(헤더가 1행)
        end_row   = 1 + total_rows  # 2행 + (total_rows - 1)

        range_notation = f"D{start_row}:F{end_row}"


        # 3) batch_update() 호출
        #    requests 구조: [{range: "...", values: [...]}]
        requests_body = [
            {
                "range": range_notation,
                "values": updated_vals_for_def
            }
        ]
        # ws_new.batch_update(...)도 가능하지만,
        # sheet단위(gspread <-> Worksheet object)에는 아래처럼:
        ws_new.batch_update(
            requests_body,
            value_input_option="USER_ENTERED"
        )

        st.success(f"곡비 파일('{new_ym}' 탭) 수정 완료! (batch_update 사용, 수식 보존)")
        st.session_state["song_cost_prepared"] = True



# ----------------------------------------------------------------
# 웹 UI 섹션1~3 부분
# ----------------------------------------------------------------
def section_one_report_input():
    """
    1번 섹션: 진행기간(YYYYMM), 보고서 발행 날짜 입력 + 보고서 생성 버튼
    """
    st.subheader("1) 정산 보고서 정보 입력 항목")

    # session_state에서 기본값 불러오기 (없으면 "")
    default_ym = st.session_state.get("ym", "")
    default_report_date = st.session_state.get("report_date", "")

    # 1. 입력 필드
    ym = st.text_input("진행기간(YYYYMM)", default_ym)
    report_date = st.text_input("보고서 발행 날짜 (YYYY-MM-DD)", default_report_date)

    # (A) 진행률 바 / 현재 처리중 아티스트 표시용 placeholder
    progress_bar = st.empty()
    artist_placeholder = st.empty()

    # 2. 생성 버튼
    if st.button("정산 보고서 생성 시작"):
        # A 계정 인증
        creds_a = get_credentials_from_secrets("A")
        gc_a = gspread.authorize(creds_a)
        drive_svc_a = build("drive", "v3", credentials=creds_a)
        sheet_svc_a = build("sheets", "v4", credentials=creds_a)

        # 유효성 체크
        if not re.match(r'^\d{6}$', ym):
            st.error("진행기간은 YYYYMM 6자리로 입력해야 합니다.")
            return
        if not report_date:
            st.error("보고서 발행 날짜를 입력하세요.")
            return

        # session_state에 입력값 저장
        st.session_state["ym"] = ym
        st.session_state["report_date"] = report_date

        # 실제 generate_report() 호출
        check_dict = {
            "song_artists": [],
            "revenue_artists": [],
            "artist_compare_result": {}
        }

        # 보고서 생성
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
        st.success(f"보고서 생성 완료! file_id={out_file_id}")

        if out_file_id:
            artist_placeholder.success("모든 아티스트 정산 보고서 생성 완료!")
            time.sleep(1)
            artist_placeholder.empty()
            progress_bar.empty()

            st.session_state["report_done"] = True
            st.session_state["report_file_id"] = out_file_id
            st.session_state["check_dict"] = check_dict


def section_two_sheet_link_and_verification():
    """
    2번 섹션: 구글시트 링크 + 검증 결과 표시 (탭 2개)
    - report_done == True 인 경우에만 표시
    """
    if "report_done" in st.session_state and st.session_state["report_done"]:
        st.subheader("2) 정산 보고서 시트링크 및 검증")

        # 탭 생성
        tab1, tab2 = st.tabs(["보고서 링크 / 요약", "세부 검증 내용"])

        # -------------------------
        # (A) 첫 번째 탭: 링크/요약
        # -------------------------
        with tab1:
            out_file_id = st.session_state.get("report_file_id", "")
            if out_file_id:
                gsheet_url = f"https://docs.google.com/spreadsheets/d/{out_file_id}/edit"
                st.write(f"**생성된 구글시트 링크:** {gsheet_url}")

            cd = st.session_state.get("check_dict", {})
            if cd:
                ar = cd.get("artist_compare_result", {})
                st.write("**검증 요약**")
                if ar:
                    st.write(f"- Song cost 아티스트 수 = {ar.get('song_count')}")
                    st.write(f"- Revenue 아티스트 수 = {ar.get('revenue_count')}")
                    st.write(f"- 공통 아티스트 수 = {ar.get('common_count')}")
                    if ar.get("missing_in_song"):
                        st.warning(f"Song에 없고 Revenue에만 있는: {ar['missing_in_song']}")
                    if ar.get("missing_in_revenue"):
                        st.warning(f"Revenue에 없고 Song에만 있는: {ar['missing_in_revenue']}")
                    
                    ver_sum = cd.get("verification_summary", {})
                    if not ver_sum:
                        st.info("추가 검증 데이터가 없습니다.")
                    else:
                        total_err = ver_sum.get("total_errors", 0)
                        artists_err = ver_sum.get("artist_error_list", [])
                        if total_err == 0:
                            st.success("모든 항목이 정상 계산되었습니다. (오류 0건)")
                        else:
                            st.error(f"총 {total_err}건의 계산 오류 발생")
                            if artists_err:
                                st.warning(f"문제 발생 아티스트: {list(set(artists_err))}")
                else:
                    st.write("검증 결과 데이터가 없습니다.")
            else:
                st.write("검증 dict가 없습니다.")

        # -------------------------
        # (B) 두 번째 탭: 세부 검증
        # -------------------------
        with tab2:
            st.write("### 세부 검증 내용")
            st.info("인풋데이터 vs. 산출결과 비교")
            
            show_detailed_verification()
    else:
        st.warning("정산 보고서 생성이 완료되면 이 섹션이 표시됩니다.")


def section_three_upload_and_split_excel():
    """
    3) '엑셀(.xlsx)' 파일 업로드 후, 시트별로 분할 & ZIP 다운로드 (서식 유지)
    """
    if "report_done" in st.session_state and st.session_state["report_done"]:
        st.subheader("3) '엑셀(.xlsx)' 파일 업로드 후, 시트별로 분할 / ZIP 다운로드 (서식 유지)")

        st.write("""
        **사용 순서**  
        1. 생성된 구글시트(보고서)에서 "**파일 → 다운로드 → Microsoft Excel (.xlsx)**"로 다운로드  
        2. 아래 업로드 버튼으로 방금 받은 .xlsx 파일을 업로드  
        3. 시트별로 분할된 XLSX 파일들을 ZIP으로 묶어 다운로드
        """)

        uploaded_file = st.file_uploader("엑셀 파일 업로드", type=["xlsx"])

        if uploaded_file is not None:
            # (C) 진행률 표시용
            progress_bar = st.progress(0.0)
            progress_text = st.empty()

            # A) 업로드된 엑셀파일 전체를 BytesIO로 보관
            original_file_data = uploaded_file.read()

            # B) 한번 로드해서 시트명 리스트(순서 등) 파악
            try:
                wb = openpyxl.load_workbook(io.BytesIO(original_file_data))
            except Exception as e:
                st.error(f"엑셀 파일을 읽는 중 오류가 발생했습니다: {e}")
                return

            sheet_names = wb.sheetnames
            total_sheets = len(sheet_names)
            if total_sheets == 0:
                st.warning("업로드된 엑셀 파일에 시트가 없습니다.")
                return

            # (D) ZIP 버퍼 준비
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                for i, sheet_name in enumerate(sheet_names):
                    # 진행률 계산
                    ratio = (i + 1) / total_sheets
                    percent = int(ratio * 100)

                    # 진행 상황 표시
                    progress_bar.progress(ratio)
                    progress_text.info(f"{percent}% 완료 - 시트 '{sheet_name}' 처리 중...")

                    # 시트별로 분할 → temp_wb 에서 불필요한 시트 제거 후 저장
                    temp_wb = openpyxl.load_workbook(io.BytesIO(original_file_data))
                    for s in temp_wb.sheetnames:
                        if s != sheet_name:
                            ws_remove = temp_wb[s]
                            temp_wb.remove(ws_remove)

                    single_buf = io.BytesIO()
                    temp_wb.save(single_buf)
                    single_buf.seek(0)

                    safe_sheet_name = sheet_name.replace("/", "_").replace("\\", "_")
                    zf.writestr(f"{safe_sheet_name}.xlsx", single_buf.getvalue())

            zip_buf.seek(0)

            # (E) 완료 메시지 + 다운로드 버튼
            progress_text.success("모든 시트 분할 완료!")
            st.download_button(
                label="ZIP 다운로드",
                data=zip_buf.getvalue(),
                file_name="split_sheets.zip",
                mime="application/zip"
            )
    else:
        st.info("정산 보고서가 먼저 생성된 뒤에, 엑셀을 업로드할 수 있습니다.")


# ----------------------------------------------------------------
# 검증(비교) 관련 헬퍼
# ----------------------------------------------------------------

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

    next_ym = get_next_month_str(ym)
    new_ws = duplicate_worksheet_with_new_name(song_cost_sh, ym, next_ym)
    new_data = new_ws.get_all_values()
    if not new_data:
        print(f"복제된 '{next_ym}' 탭이 비어 있습니다.")
        return

    new_header = new_data[0]
    try:
        idx_artist_new = new_header.index("아티스트명")
        idx_prev_new   = new_header.index("전월 잔액")
        idx_deduct_new = new_header.index("당월 차감액")
        idx_remain_new = new_header.index("당월 잔액")
    except ValueError:
        print("새로 만든 시트(다음 달 탭)에 필요한 칼럼이 없습니다.")
        return

    content = new_data[1:]
    updated = []
    for row in content:
        row_data = row[:]
        artist_name_new = row_data[idx_artist_new].strip()
        if artist_name_new in prev_month_dict:
            row_data[idx_prev_new] = str(prev_month_dict[artist_name_new])
        row_data[idx_deduct_new] = "0"
        updated.append(row_data)

    if updated:
        new_ws.update(
            range_name="A2",
            values=updated,
            value_input_option="USER_ENTERED"
        )
    print(f"'{ym}' → '{next_ym}' 탭 복제 및 전월/당월 잔액 세팅 완료!")



# ========== [4] 핵심 로직: generate_report =============
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
    [요약]
    1) input_song cost / input_online revenue 시트에서 해당 ym 데이터를 읽어옴
    2) 아티스트별 매출 및 곡비(전월+당월 발생액, 당월차감 등) 정보를 합산
    3) 구글 스프레드시트 형태의 'output_report_YYYYMM'을 생성하여
       - 각 아티스트별 (1) 세부매출내역 탭, (2) 정산서 탭 생성
       - '정산서' 탭 내 '3. 공제 내역' 칼럼 중 '곡비'를 (전월 잔액 + 당월 발생액)으로 표기
    4) 최종 검증 정보를 check_dict에 누적
    5) 작업 완료 후 out_file_id(생성된 구글시트 ID) 반환
    """

    folder_id = st.secrets["google_service_account_a"]["folder_id"]

    # ------------------- (A) input_song cost -------------------
    try:
        song_cost_sh = gc.open("input_song cost")
    except gspread.exceptions.SpreadsheetNotFound:
        st.error("Google Sheet 'input_song cost'를 찾을 수 없습니다.")
        return ""

    ws_map_sc = {ws.title: ws for ws in song_cost_sh.worksheets()}
    if ym not in ws_map_sc:
        st.error(f"input_song cost에 '{ym}' 탭이 없습니다.")
        return ""
    ws_sc = ws_map_sc[ym]
    data_sc = ws_sc.get_all_values()
    if not data_sc:
        st.error(f"'{ym}' 탭이 비어있습니다.")
        return ""
    header_sc = data_sc[0]
    # 마지막 합계/총계 행은 제외하고 읽는 경우:
    rows_sc = data_sc[1:-1]

    # 이번에 '당월 발생액' 칼럼까지 사용하므로 인덱스 추가
    try:
        idx_artist = header_sc.index("아티스트명")
        idx_rate   = header_sc.index("정산 요율")
        idx_prev   = header_sc.index("전월 잔액")
        idx_curr   = header_sc.index("당월 발생액")
        idx_deduct = header_sc.index("당월 차감액")
        idx_remain = header_sc.index("당월 잔액")
    except ValueError as e:
        st.error(f"[input_song cost] 시트 칼럼 명이 맞는지 확인 필요: {e}")
        return ""

    # 숫자로 변환하는 헬퍼
    def to_num(x: str) -> float:
        if not x:
            return 0.0
        return float(x.replace("%", "").replace(",", ""))


    # 아티스트별 곡비 정보
    #   → '전월 잔액'(prev), '당월 발생액'(curr), '당월 차감'(deduct), '당월 잔액'(remain), '정산요율'(rate)
    #   (실제 작업에서는 나중에 '곡비' = prev + curr)
    artist_cost_dict = {}
    for row in rows_sc:
        artist_name = row[idx_artist].strip()
        if not artist_name:
            continue
        cost_data = {
            "정산요율": to_num(row[idx_rate]),
            "전월잔액": to_num(row[idx_prev]),
            "당월발생": to_num(row[idx_curr]),
            "당월차감액": to_num(row[idx_deduct]),
            "당월잔액": to_num(row[idx_remain])
        }
        artist_cost_dict[artist_name] = cost_data


    # ------------------- (B) input_online revenue -------------
    try:
        revenue_sh = gc.open("input_online revenue")
    except gspread.exceptions.SpreadsheetNotFound:
        st.error("Google Sheet 'input_online revenue'를 찾을 수 없습니다.")
        return ""

    ws_map_or = {ws.title: ws for ws in revenue_sh.worksheets()}
    if ym not in ws_map_or:
        st.error(f"input_online revenue에 '{ym}' 탭이 없습니다.")
        return ""
    ws_or = ws_map_or[ym]
    data_or = ws_or.get_all_values()
    if not data_or:
        st.error(f"{ym} 탭이 비어있습니다.")
        return ""

    header_or = data_or[0]
    rows_or = data_or[1:]
    try:
        col_aartist = header_or.index("앨범아티스트")
        col_album   = header_or.index("앨범명")
        col_major   = header_or.index("대분류")
        col_middle  = header_or.index("중분류")
        col_service = header_or.index("서비스명")
        col_revenue = header_or.index("권리사정산금액")
    except ValueError as e:
        st.error(f"[input_online revenue] 시트 칼럼 명이 맞는지 확인 필요: {e}")
        return ""

    # 아티스트별 매출 정보
    from collections import defaultdict
    artist_revenue_dict = defaultdict(list)
    for row in rows_or:
        a = row[col_aartist].strip()
        if not a:
            continue
        alb = row[col_album]
        maj = row[col_major]
        mid = row[col_middle]
        srv = row[col_service]
        try:
            rv_val = float(row[col_revenue].replace(",", ""))
        except:
            rv_val = 0.0
        artist_revenue_dict[a].append({
            "album": alb,
            "major": maj,
            "middle": mid,
            "service": srv,
            "revenue": rv_val
        })


    # ---------------------------------------------------------
    # [추가] check_dict 내부 구조 확인 / 초기화
    # ---------------------------------------------------------
    if "verification_summary" not in check_dict:
        check_dict["verification_summary"] = {
            "total_errors": 0,
            "artist_error_list": []
        }
    if "details_verification" not in check_dict:
        check_dict["details_verification"] = {
            "정산서": [],
            "세부매출": []
        }
    if "details_per_artist" not in check_dict:
        check_dict["details_per_artist"] = {}

    # 아티스트 목록 검증
    song_artists = [r[idx_artist] for r in rows_sc if r[idx_artist]]
    revenue_artists = [r[col_aartist].strip() for r in rows_or if r[col_aartist].strip()]
    check_dict["song_artists"] = song_artists
    check_dict["revenue_artists"] = revenue_artists

    compare_res = compare_artists(song_artists, revenue_artists)
    check_dict["artist_compare_result"] = compare_res


    # ------------------- (C) 아티스트 목록 ---------------
    all_artists = sorted(set(artist_cost_dict.keys()) | set(artist_revenue_dict.keys()))
    all_artists = [a for a in all_artists if a and a not in ("합계", "총계")]
    st.session_state["all_artists"] = all_artists


    # ------------------- (D) output_report_YYYYMM --------
    out_filename = f"ouput_report_{ym}"
    out_file_id = create_new_spreadsheet(out_filename, folder_id, drive_svc)
    out_sh = gc.open_by_key(out_file_id)
    
    # 기본생성 sheet1 삭제 시도
    try:
        out_sh.del_worksheet(out_sh.worksheet("Sheet1"))
    except:
        pass

    year_val = ym[:4]
    month_val = ym[4:]

    # (UI) 진행률 표시용
    progress_bar.progress(0)
    artist_placeholder.info("아티스트 보고서 생성 중...")

    # 시트 생성(batch)
    needed_titles = []
    for artist in all_artists:
        needed_titles.append(f"{artist}(세부매출내역)")
        needed_titles.append(f"{artist}(정산서)")
    batch_add_sheets(out_file_id, sheet_svc, needed_titles)


    # ===================================================================
    # (E) 아티스트별로 (1) 세부매출내역 탭, (2) 정산서 탭 생성
    # ===================================================================

    all_requests = []  # batchUpdate requests 모음

    for i, artist in enumerate(all_artists):
        # 진행률
        ratio = (i + 1) / len(all_artists)
        progress_bar.progress(ratio)
        artist_placeholder.info(f"[{i+1}/{len(all_artists)}] '{artist}' 처리 중...")

        # (1) 세부매출내역 탭
        ws_detail = out_sh.worksheet(f"{artist}(세부매출내역)")
        details = artist_revenue_dict[artist]
        details_sorted = sorted(details, key=lambda d: album_sort_key(d["album"]))

        detail_matrix = []
        detail_matrix.append(["앨범아티스트","앨범명","대분류","중분류","서비스명","기간","매출 순수익"])

        total_det = 0
        for d in details_sorted:
            rv = d["revenue"]
            total_det += rv
            detail_matrix.append([
                artist,
                d["album"],
                d["major"],
                d["middle"],
                d["service"],
                f"{year_val}년 {month_val}월",
                to_currency(rv)
            ])

        # 합계
        detail_matrix.append(["합계","","","","","", to_currency(total_det)])
        row_cursor_detail_end = len(detail_matrix)

        # 시트 업데이트
        ws_detail.update("A1", detail_matrix)
        time.sleep(1)

        # 세부매출내역 탭에 대한 서식/테두리 등 batch 요청
        detail_requests = []
        sheet_id_detail = ws_detail.id

        # (A) 시트 크기(row_cursor_detail_end, 7열)
        detail_requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id_detail,
                    "gridProperties": {
                        "rowCount": row_cursor_detail_end,
                        "columnCount": 7
                    }
                },
                "fields": "gridProperties(rowCount,columnCount)"
            }
        })

        # (B) 열너비 설정 (A=0, B=1, ...)
        # 예: A열(0) → 140, B열(1) → 140, E열(4) → 120
        detail_requests.extend([
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id_detail,
                        "dimension": "COLUMNS",
                        "startIndex": 0, 
                        "endIndex": 1
                    },
                    "properties": {"pixelSize": 140},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id_detail,
                        "dimension": "COLUMNS",
                        "startIndex": 1,
                        "endIndex": 2
                    },
                    "properties": {"pixelSize": 140},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id_detail,
                        "dimension": "COLUMNS",
                        "startIndex": 4,
                        "endIndex": 5
                    },
                    "properties": {"pixelSize": 120},
                    "fields": "pixelSize"
                }
            },
        ])

        # (C) 헤더(A1~G1) 포맷
        detail_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id_detail,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red":1.0, "green":0.8, "blue":0.0},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": {"red":0,"green":0,"blue":0}
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })

        # (D) 합계행 병합 + 서식
        sum_row_0based = row_cursor_detail_end - 1
        detail_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id_detail,
                    "startRowIndex": sum_row_0based,
                    "endRowIndex": sum_row_0based+1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 6
                },
                "mergeType": "MERGE_ALL"
            }
        })
        detail_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id_detail,
                    "startRowIndex": sum_row_0based,
                    "endRowIndex": sum_row_0based+1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 6
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red":1.0,"green":0.8,"blue":0.0},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"bold": True}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # 합계값(G열)에 오른쪽 정렬
        detail_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id_detail,
                    "startRowIndex": sum_row_0based,
                    "endRowIndex": sum_row_0based+1,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": True}
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,textFormat)"
            }
        })
        # 매출 순수익 칼럼 (F열=idx=6) 나머지 행들
        detail_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id_detail,
                    "startRowIndex": 1,
                    "endRowIndex": sum_row_0based,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "RIGHT"
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment)"
            }
        })

        # (E) 전체 테두리
        detail_requests.append({
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id_detail,
                    "startRowIndex": 0,
                    "endRowIndex": row_cursor_detail_end,
                    "startColumnIndex": 0,
                    "endColumnIndex": 7
                },
                "top":    {"style":"SOLID","width":1},
                "bottom": {"style":"SOLID","width":1},
                "left":   {"style":"SOLID","width":1},
                "right":  {"style":"SOLID","width":1},
                "innerHorizontal": {"style":"SOLID","width":1},
                "innerVertical":   {"style":"SOLID","width":1}
            }
        })

        all_requests.extend(detail_requests)

        # (호출 횟수 분할) 1회 batchUpdate 요청이 너무 커지면 나눠서 전송
        if len(all_requests) >= 200:
            sheet_svc.spreadsheets().batchUpdate(
                spreadsheetId=out_file_id,
                body={"requests": all_requests}
            ).execute()
            all_requests.clear()
            time.sleep(1)


        # ------------------------------------------------------
        # 정산서 탭 (batchUpdate 방식)
        # ------------------------------------------------------
        ws_report = out_sh.worksheet(f"{artist}(정산서)")
        ws_report_id = ws_report.id

        # 매출 합
        sum_1 = sum(d["revenue"] for d in details_sorted)  # "음원서비스별" 총합
        # 앨범별 합
        album_sum = defaultdict(float)
        for d in details_sorted:
            album_sum[d["album"]] += d["revenue"]
        sum_2 = sum(album_sum.values())

        # (A) "곡비" = "전월 잔액 + 당월 발생액" (요청 사항)
        prev_val = artist_cost_dict[artist]["전월잔액"]
        curr_val = artist_cost_dict[artist]["당월발생"]
        # 보고서 '3. 공제 내역'의 '곡비' 칼럼 값 = prev_val + curr_val
        song_cost_for_report = prev_val + curr_val

        # (B) 공제 금액 & 잔액
        deduct_val = artist_cost_dict[artist]["당월차감액"]  # 이미 input_song cost에서 계산된 값
        remain_val = artist_cost_dict[artist]["당월잔액"]   # 동일
        # "공제 적용 후" 매출 = (음원 매출 합) - 공제금액 => sum_2 - deduct_val
        # (단, 요청 사항/업무로직에 따라 정확히 어떻게 적용할지는 케이스별로 맞춤)

        # (C) 정산율 / 최종 정산금액
        rate_val = artist_cost_dict[artist]["정산요율"]
        공제적용후 = sum_2 - deduct_val
        final_amount = 공제적용후 * (rate_val / 100.0)
        

        # --------------------------------------
        # 정산서 테이블(직접 row col 배열 채우기)
        # --------------------------------------
        report_matrix = []
        for _ in range(300):
            report_matrix.append([""] * 8)

        # 1) 상단 공통정보
        report_matrix[1][6] = report_date   # 보고서 발행일
        report_matrix[3][1] = f"{year_val}년 {month_val}월 판매분"
        report_matrix[5][1] = f"{artist}님 음원 정산 내역서"

        report_matrix[7][0] = "•"
        report_matrix[7][1] = "저희와 함께해 주셔서 정말 감사하고 앞으로도 잘 부탁드리겠습니다!"
        report_matrix[8][0] = "•"
        report_matrix[8][1] = f"{year_val}년 {month_val}월 음원의 수익을 아래와 같이 정산드리오니 참고 부탁드립니다."
        report_matrix[9][0] = "•"
        report_matrix[9][1] = "정산 관련하여 문의사항이 있다면 무엇이든, 언제든 편히 메일 주세요!"
        report_matrix[9][5] = "E-Mail : lucasdh3013@naver.com"

        # -----------------------------------------------------------------
        # 1. 음원 서비스별 정산내역 (세부매출 그대로)
        # -----------------------------------------------------------------
        report_matrix[12][0] = "1."
        report_matrix[12][1] = "음원 서비스별 정산내역"

        header_row_1 = 13
        headers_1 = ["앨범", "대분류", "중분류", "서비스명", "기간", "매출액"]
        for i_h, val_h in enumerate(headers_1):
            report_matrix[header_row_1][1 + i_h] = val_h

        row_cursor = header_row_1 + 1
        for d in details_sorted:
            rv = d["revenue"]
            report_matrix[row_cursor][1] = d["album"]
            report_matrix[row_cursor][2] = d["major"]
            report_matrix[row_cursor][3] = d["middle"]
            report_matrix[row_cursor][4] = d["service"]
            report_matrix[row_cursor][5] = f"{year_val}년 {month_val}월"
            report_matrix[row_cursor][6] = to_currency(rv)
            row_cursor += 1

        # 합계
        report_matrix[row_cursor][1] = "합계"
        report_matrix[row_cursor][6] = to_currency(sum_1)
        row_cursor_sum1 = row_cursor
        row_cursor += 2

        # -----------------------------------------------------------------
        # 2. 앨범 별 정산 내역
        # -----------------------------------------------------------------
        report_matrix[row_cursor][0] = "2."
        report_matrix[row_cursor][1] = "앨범 별 정산 내역"
        row_cursor += 1
        row_cursor_album = row_cursor
        report_matrix[row_cursor][1] = "앨범"
        report_matrix[row_cursor][5] = "기간"
        report_matrix[row_cursor][6] = "매출액"
        row_cursor += 1

        for alb in sorted(album_sum.keys(), key=album_sort_key):
            amt = album_sum[alb]
            report_matrix[row_cursor][1] = alb
            report_matrix[row_cursor][5] = f"{year_val}년 {month_val}월"
            report_matrix[row_cursor][6] = to_currency(amt)
            row_cursor += 1

        report_matrix[row_cursor][1] = "합계"
        report_matrix[row_cursor][6] = to_currency(sum_2)
        row_cursor_sum2 = row_cursor
        row_cursor += 2

        # -----------------------------------------------------------------
        # 3. 공제 내역
        #    (요청사항: '곡비' 칼럼 = (전월 잔액 + 당월 발생액))
        # -----------------------------------------------------------------
        report_matrix[row_cursor][0] = "3."
        report_matrix[row_cursor][1] = "공제 내역"
        row_cursor += 1
        row_cursor_deduction = row_cursor

        report_matrix[row_cursor][1] = "앨범"
        report_matrix[row_cursor][2] = "곡비"
        report_matrix[row_cursor][3] = "공제 금액"
        report_matrix[row_cursor][5] = "공제 후 남은 곡비"
        report_matrix[row_cursor][6] = "공제 적용 금액"
        row_cursor += 1

        # 앨범(들)을 표기만 할지, 혹은 여러줄로 표현할지 등은 업무 규칙에 따라
        alb_list = sorted(album_sum.keys(), key=album_sort_key)
        alb_str = ", ".join(alb_list) if alb_list else "(앨범 없음)"

        report_matrix[row_cursor][1] = alb_str
        # (중요) 여기서 "곡비" = prev_val + curr_val
        report_matrix[row_cursor][2] = to_currency(song_cost_for_report)
        # 공제금액
        report_matrix[row_cursor][3] = to_currency(deduct_val)
        # 공제 후 남은 곡비
        report_matrix[row_cursor][5] = to_currency(remain_val)
        # 공제 적용 금액 (매출 - 공제금액)
        report_matrix[row_cursor][6] = to_currency(sum_2 - deduct_val)
        row_cursor += 2
        row_cursor_sum3 = row_cursor

        # -----------------------------------------------------------------
        # 4. 수익 배분
        # -----------------------------------------------------------------
        report_matrix[row_cursor][0] = "4."
        report_matrix[row_cursor][1] = "수익 배분"
        row_cursor += 1
        row_cursor_rate = row_cursor
        report_matrix[row_cursor][1] = "앨범"
        report_matrix[row_cursor][2] = "항목"
        report_matrix[row_cursor][3] = "적용율"
        report_matrix[row_cursor][6] = "적용 금액"
        row_cursor += 1

        report_matrix[row_cursor][1] = alb_str
        report_matrix[row_cursor][2] = "수익 배분율"
        report_matrix[row_cursor][3] = f"{int(rate_val)}%"
        report_matrix[row_cursor][6] = to_currency(final_amount)
        row_cursor += 1

        report_matrix[row_cursor][1] = "총 정산금액"
        report_matrix[row_cursor][6] = to_currency(final_amount)
        row_cursor_sum4 = row_cursor
        row_cursor += 2

        report_matrix[row_cursor][6] = "* 부가세 별도"
        row_cursor_report_end = row_cursor + 2

        # 시트에 실제 업로드
        ws_report.update("A1", report_matrix)
        time.sleep(1)

        # ------------------------------------
        # (검증) check_dict에 비교결과 반영
        # ------------------------------------
        # (1) 세부매출 vs 정산서
        for d in details_sorted:
            original_val = d["revenue"]
            report_val   = d["revenue"]  # 현재는 동일
            is_match = almost_equal(original_val, report_val)
            if not is_match:
                check_dict["verification_summary"]["total_errors"] += 1
                check_dict["verification_summary"]["artist_error_list"].append(artist)

            row_report_item = {
                "아티스트": artist,
                "구분": "음원서비스별매출",
                "앨범": d["album"],
                "서비스명": d["service"],
                "원본_매출액": original_val,
                "정산서_매출액": report_val,
                "match_매출액": is_match,
            }
            check_dict["details_verification"]["세부매출"].append(row_report_item)

        # (2) 공제 내역(곡비,공제금액,공제후잔액)
        #   원본(= input_song cost) 값 vs 보고서 값
        #   "곡비"는 (prev + curr), "공제금액"=deduct_val, "남은 곡비"=remain_val
        #   *원본_곡비 = (전월잔액 + 당월발생)
        original_song_cost = artist_cost_dict[artist]["전월잔액"] + artist_cost_dict[artist]["당월발생"]
        is_match_songcost = almost_equal(original_song_cost, song_cost_for_report)
        is_match_deduct   = almost_equal(artist_cost_dict[artist]["당월차감액"], deduct_val)
        is_match_remain   = almost_equal(artist_cost_dict[artist]["당월잔액"], remain_val)

        if not (is_match_songcost and is_match_deduct and is_match_remain):
            check_dict["verification_summary"]["total_errors"] += 1
            check_dict["verification_summary"]["artist_error_list"].append(artist)

        row_report_item_3 = {
            "아티스트": artist,
            "구분": "공제내역",
            # 곡비
            "원본_곡비": original_song_cost,
            "정산서_곡비": song_cost_for_report,
            "match_곡비": is_match_songcost,
            # 공제금액
            "원본_공제금액": artist_cost_dict[artist]["당월차감액"],
            "정산서_공제금액": deduct_val,
            "match_공제금액": is_match_deduct,
            # 공제후잔액
            "원본_공제후잔액": artist_cost_dict[artist]["당월잔액"],
            "정산서_공제후잔액": remain_val,
            "match_공제후잔액": is_match_remain,
        }
        check_dict["details_verification"]["정산서"].append(row_report_item_3)

        # (3) 4번 수익 배분율
        original_rate = artist_cost_dict[artist]["정산요율"]
        is_rate_match = almost_equal(original_rate, rate_val)
        if not is_rate_match:
            check_dict["verification_summary"]["total_errors"] += 1
            check_dict["verification_summary"]["artist_error_list"].append(artist)

        row_report_item_4 = {
            "아티스트": artist,
            "구분": "수익배분율",
            "원본_정산율(%)": original_rate,
            "정산서_정산율(%)": rate_val,
            "match_정산율": is_rate_match,
        }
        check_dict["details_verification"]["정산서"].append(row_report_item_4)

        time.sleep(1)   

        # --------------------------------------------------
        # 정산서 탭(디자인/서식) batchUpdate
        # --------------------------------------------------
        report_requests = []

        # (A) 시트 row/col 크기
        report_requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws_report_id,
                    "gridProperties": {
                        "rowCount": row_cursor_report_end,
                        "columnCount": 8
                    }
                },
                "fields": "gridProperties(rowCount,columnCount)"
            }
        })

        # (B) 열너비 (A=0 ~ H=7)
        report_requests.extend([
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": 1
                    },
                    "properties": {"pixelSize": 40},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "COLUMNS",
                        "startIndex": 1,
                        "endIndex": 2
                    },
                    "properties": {"pixelSize": 200},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "COLUMNS",
                        "startIndex": 2,
                        "endIndex": 3
                    },
                    "properties": {"pixelSize": 130},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "COLUMNS",
                        "startIndex": 3,
                        "endIndex": 4
                    },
                    "properties": {"pixelSize": 120},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "COLUMNS",
                        "startIndex": 4,
                        "endIndex": 5
                    },
                    "properties": {"pixelSize": 130},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "COLUMNS",
                        "startIndex": 5,
                        "endIndex": 6
                    },
                    "properties": {"pixelSize": 130},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "COLUMNS",
                        "startIndex": 6,
                        "endIndex": 7
                    },
                    "properties": {"pixelSize": 130},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "COLUMNS",
                        "startIndex": 7,
                        "endIndex": 8
                    },
                    "properties": {"pixelSize": 40},
                    "fields": "pixelSize"
                }
            },
        ])

        # (C) 특정행 높이
        report_requests.extend([
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "ROWS",
                        "startIndex": 3,
                        "endIndex": 4
                    },
                    "properties": {"pixelSize": 30},
                    "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws_report_id,
                        "dimension": "ROWS",
                        "startIndex": 5,
                        "endIndex": 6
                    },
                    "properties": {"pixelSize": 30},
                    "fields": "pixelSize"
                }
            },
        ])

        # (D) 상단 고정 항목(발행 날짜, H2: row=1, col=6)
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 1,
                    "endRowIndex": 2,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "RIGHT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })        

        # (E) 상단 고정 항목(판매분, B4:E4)
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 3,  # (4-1)
                    "endRowIndex": 4,
                    "startColumnIndex": 1,  # (B=1)
                    "endColumnIndex": 5     # (E=4 => endIndex=5)
                },
                "mergeType": "MERGE_ALL"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 3,
                    "endRowIndex": 4,
                    "startColumnIndex": 1,
                    "endColumnIndex": 5
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 15,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })

        # (F) 상단 고정 항목(아티스트 정산내역서, B6:G6)
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 5,
                    "endRowIndex": 6,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "mergeType": "MERGE_ALL"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 5,
                    "endRowIndex": 6,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.896, "green": 0.988, "blue": 1},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 15,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })

        # (G) 상단 고정 항목(안내문, B8:E8~B10:E10)
        #8행
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 7,  # (4-1)
                    "endRowIndex": 8,
                    "startColumnIndex": 1,  # (B=1)
                    "endColumnIndex": 5     # (E=4 => endIndex=5)
                },
                "mergeType": "MERGE_ALL"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 7,  # (4-1)
                    "endRowIndex": 8,
                    "startColumnIndex": 1,  # (B=1)
                    "endColumnIndex": 5     # (E=4 => endIndex=5)
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        #9행
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 8,  
                    "endRowIndex": 9,
                    "startColumnIndex": 1,
                    "endColumnIndex": 5 
                },
                "mergeType": "MERGE_ALL"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 8,  
                    "endRowIndex": 9,
                    "startColumnIndex": 1,
                    "endColumnIndex": 5 
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                          "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        #10행
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 9,  
                    "endRowIndex": 10,
                    "startColumnIndex": 1,
                    "endColumnIndex": 5 
                },
                "mergeType": "MERGE_ALL"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 9,  
                    "endRowIndex": 10,
                    "startColumnIndex": 1,
                    "endColumnIndex": 5 
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # 10행 (E-Mail 칸)
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 9,  
                    "endRowIndex": 10,
                    "startColumnIndex": 5,
                    "endColumnIndex": 7 
                },
                "mergeType": "MERGE_ALL"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 9,
                    "endRowIndex": 10,
                    "startColumnIndex": 5,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "RIGHT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "foregroundColor": {"red": 0.29, "green": 0.53, "blue": 0.91},
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        
        # (H) 1열 정렬 (번호 영역)
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 1,
                    "endRowIndex": row_cursor_rate+1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })

        # (I) 하단 고정 항목(부가세, G)
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_report_end-2,
                    "endRowIndex": row_cursor_report_end,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "RIGHT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        }) 
    

        # (J-1) "음원 서비스별 정산내역" 표 타이틀
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 12,  # (4-1)
                    "endRowIndex": 13,
                    "startColumnIndex": 1,  # (B=1)
                    "endColumnIndex": 2     # (E=4 => endIndex=5)
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (J-2) "음원 서비스별 정산내역" 표 헤더 (Row=13)
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 13,
                    "endRowIndex": 14,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.3, "green": 0.82, "blue": 0.88},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (J-3) 합계행 전 병합
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum1-2,
                    "endRowIndex": row_cursor_sum1-1,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "mergeType": "MERGE_ALL"
            }
        })
        # (J-4) 합계행 병합
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum1-1,
                    "endRowIndex": row_cursor_sum1,
                    "startColumnIndex": 1,
                    "endColumnIndex": 6
                },
                "mergeType": "MERGE_ALL"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum1-1,
                    "endRowIndex": row_cursor_sum1,
                    "startColumnIndex": 1,
                    "endColumnIndex": 6
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.896, "green": 0.988, "blue": 1},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum1-1,
                    "endRowIndex": row_cursor_sum1,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.896, "green": 0.988, "blue": 1},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (J-5) 표에 Banding (줄무늬 효과)
        banding_start_row = 14
        banding_end_row = row_cursor_sum1 - 2
        banding_start_col = 1
        banding_end_col = 7
        if banding_end_row > banding_start_row:  # 유효범위 체크
            report_requests.append({
                "addBanding": {
                    "bandedRange": {
                        "range": {
                            "sheetId": ws_report_id,
                            "startRowIndex": banding_start_row,
                            "endRowIndex": banding_end_row,
                            "startColumnIndex": banding_start_col,
                            "endColumnIndex": banding_end_col
                        },
                        "rowProperties": {
                            "firstBandColor": {
                                "red": 1.0, "green": 1.0, "blue": 1.0
                            },
                            "secondBandColor": {
                                "red": 0.896, "green": 0.988, "blue": 1
                            }
                        },
                        
                    }
                }
            })
            report_requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": ws_report_id,
                        "startRowIndex": banding_start_row,
                        "endRowIndex": banding_end_row,
                        "startColumnIndex": banding_start_col,
                        "endColumnIndex": banding_end_col
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "textFormat": {
                                "fontFamily": "Malgun Gothic",
                                "fontSize": 10,
                                "bold": False
                            }
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
                }
            })


        # (K-1) 앨범별 정산내역 타이틀
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_album-2,
                    "endRowIndex": row_cursor_album-1,
                    "startColumnIndex": 1, 
                    "endColumnIndex": 2    
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (K-2) 앨범별 정산내역 헤더
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_album-1,
                    "endRowIndex": row_cursor_album,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.3, "green": 0.82, "blue": 0.88},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (K-3) 앨범별 정산내역 표 본문
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_album,
                    "endRowIndex": row_cursor_sum2-2,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (K-4) 앨범별 정산내역 합계행
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum2-3,
                    "endRowIndex": row_cursor_sum2-2,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (K-5) 합계행 병합
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum2-3,
                    "endRowIndex": row_cursor_sum2-2,
                    "startColumnIndex": 1,
                    "endColumnIndex": 6
                },
                "mergeType": "MERGE_ALL"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum2-3,
                    "endRowIndex": row_cursor_sum2-2,
                    "startColumnIndex": 1,
                    "endColumnIndex": 6
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.896, "green": 0.988, "blue": 1},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum2-3,
                    "endRowIndex": row_cursor_sum2-2,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.896, "green": 0.988, "blue": 1},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })


        # (L-1) 공제 내역 타이틀
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_deduction-2,  # (4-1)
                    "endRowIndex": row_cursor_deduction-1,
                    "startColumnIndex": 1,  # (B=1)
                    "endColumnIndex": 2     # (E=4 => endIndex=5)
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (L-2) 공제 내역 헤더
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_deduction-1,
                    "endRowIndex": row_cursor_deduction,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.3, "green": 0.82, "blue": 0.88},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (L-3) 공제 내역 표 본문 (데이터부분)
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_deduction,
                    "endRowIndex": row_cursor_deduction+1,
                    "startColumnIndex": 1,
                    "endColumnIndex": 6
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (L-4) 공제 내역 표 본문 (합계 부분)
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_deduction,
                    "endRowIndex": row_cursor_deduction+1,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })


        # (M-1) 수익 배분 타이틀
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_rate-2,
                    "endRowIndex": row_cursor_rate-1,
                    "startColumnIndex": 1,  
                    "endColumnIndex": 2    
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (M-2) 수익 배분 헤더
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_rate-1,
                    "endRowIndex": row_cursor_rate,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.3, "green": 0.82, "blue": 0.88},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (M-3) 수익 배분 표 본문 
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_rate,
                    "endRowIndex": row_cursor_rate+1,
                    "startColumnIndex": 1,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": False
                        }
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # (M-4) 수익 배분 표 합계행 병합
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum4-1,
                    "endRowIndex": row_cursor_sum4,
                    "startColumnIndex": 1,
                    "endColumnIndex": 6
                },
                "mergeType": "MERGE_ALL"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum4-1,
                    "endRowIndex": row_cursor_sum4,
                    "startColumnIndex": 1,
                    "endColumnIndex": 6
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.896, "green": 0.988, "blue": 1},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum4-1,
                    "endRowIndex": row_cursor_sum4,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.896, "green": 0.988, "blue": 1},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {
                            "fontFamily": "Malgun Gothic",
                            "fontSize": 10,
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })


        # (N) 전체 테두리 화이트
        report_requests.append({
            "updateBorders": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 0,
                    "endRowIndex": row_cursor_report_end,
                    "startColumnIndex": 0,
                    "endColumnIndex": 8
                },
                "top":    {"style": "SOLID","width":1, "color":{"red":1,"green":1,"blue":1}},
                "bottom": {"style": "SOLID","width":1, "color":{"red":1,"green":1,"blue":1}},
                "left":   {"style": "SOLID","width":1, "color":{"red":1,"green":1,"blue":1}},
                "right":  {"style": "SOLID","width":1, "color":{"red":1,"green":1,"blue":1}},
                "innerHorizontal": {"style":"SOLID","width":1,"color":{"red":1,"green":1,"blue":1}},
                "innerVertical":   {"style":"SOLID","width":1,"color":{"red":1,"green":1,"blue":1}}
            }
        })
        

        # (O) 표 부분 점선 
        def add_dotted_borders(r1, r2, c1, c2):
            """바깥+안쪽 모두 DOTTED"""
            report_requests.append({
                "updateBorders": {
                    "range": {
                        "sheetId": ws_report_id,
                        "startRowIndex": r1,
                        "endRowIndex": r2,
                        "startColumnIndex": c1,
                        "endColumnIndex": c2
                    },
                    "top":    {"style": "DOTTED", "width": 1, "color":{"red":0,"green":0,"blue":0}},
                    "bottom": {"style": "DOTTED", "width": 1, "color":{"red":0,"green":0,"blue":0}},
                    "left":   {"style": "DOTTED", "width": 1, "color":{"red":0,"green":0,"blue":0}},
                    "right":  {"style": "DOTTED", "width": 1, "color":{"red":0,"green":0,"blue":0}},
                    "innerHorizontal": {"style": "DOTTED", "width": 1, "color":{"red":0,"green":0,"blue":0}},
                    "innerVertical":   {"style": "DOTTED", "width": 1, "color":{"red":0,"green":0,"blue":0}}
                }
            })
        # 1번 섹션 A14:G30 => row=13..30, col=0..7
        add_dotted_borders(13, row_cursor_sum1, 1, 7)
        # 2번 섹션 
        add_dotted_borders(row_cursor_album, row_cursor_sum2, 1, 7)
        # 3번 섹션 
        add_dotted_borders(row_cursor_deduction, row_cursor_sum3-1, 1, 7)
        # 4번 섹션 
        add_dotted_borders(row_cursor_rate, row_cursor_sum4+1, 1, 7)
        

        # (P) 시트 외곽 검정 SOLID 
        report_requests.append({
            "updateBorders": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": 0,
                    "endRowIndex": row_cursor_report_end,
                    "startColumnIndex": 0,
                    "endColumnIndex": 8
                },
                "top":    {"style": "SOLID","width":1, "color":{"red":0,"green":0,"blue":0}},
                "bottom": {"style": "SOLID","width":1, "color":{"red":0,"green":0,"blue":0}},
                "left":   {"style": "SOLID","width":1, "color":{"red":0,"green":0,"blue":0}},
                "right":  {"style": "SOLID","width":1, "color":{"red":0,"green":0,"blue":0}}
                # innerHorizontal, innerVertical는 생략 => 기존 값 유지
            }
        })
        
        all_requests.extend(report_requests)


        # batchUpdate 분할 전송
        if len(all_requests) >= 200:
            sheet_svc.spreadsheets().batchUpdate(
                spreadsheetId=out_file_id,
                body={"requests": all_requests}
            ).execute()
            all_requests.clear()
            time.sleep(1)

    # ---------------------------
    # 마지막으로 남은 요청들을 일괄 처리
    # ---------------------------
    if all_requests:
        sheet_svc.spreadsheets().batchUpdate(
            spreadsheetId=out_file_id,
            body={"requests": all_requests}
        ).execute()
        all_requests.clear()
    time.sleep(1)

    # 루프 끝나면 처리 완료 메시지 (원한다면)
    artist_placeholder.success("모든 아티스트 처리 완료!")

    # ----------------------
    # 다음달 탭 복제 (옵션)
    # ----------------------
    update_next_month_tab(song_cost_sh, ym)
    time.sleep(1)

    # 최종 결과 반환
    return out_file_id


# ========== [5] Streamlit UI =============
def main():
    st.title("아티스트 음원 정산 보고서 자동 생성기")

    # 맨 앞단 - 곡비 파일 제작/수정 섹션
    section_zero_prepare_song_cost()
    st.divider()

    # 섹션 1: 보고서 생성
    section_one_report_input()
    st.divider()

    # 섹션 2: 시트 링크 & 검증
    section_two_sheet_link_and_verification()
    st.divider()

    # 섹션 3: 엑셀 업로드 후 시트분할 ZIP 다운로드
    section_three_upload_and_split_excel()


if __name__ == "__main__":
    main()
