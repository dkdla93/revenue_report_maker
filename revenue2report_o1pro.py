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
# 웹 UI 섹션1~3 부분
# ----------------------------------------------------------------
def section_one_report_input():
    """
    1번 섹션: 진행기간(YYYYMM), 보고서 발행 날짜 입력 + 보고서 생성 버튼
    """
    st.subheader("1) 정산 보고서 정보 입력 항목")

    # (A) 진행률 바 / 현재 처리중 아티스트 표시용 placeholder
    progress_bar = st.empty()
    artist_placeholder = st.empty()

    # session_state에서 기본값 불러오기 (없으면 "")
    default_ym = st.session_state.get("ym", "")
    default_report_date = st.session_state.get("report_date", "")

    # 1. 입력 필드
    ym = st.text_input("진행기간(YYYYMM)", default_ym)
    report_date = st.text_input("보고서 발행 날짜 (YYYY-MM-DD)", default_report_date)

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

        # 실제 generate_report(...) 호출 (A계정으로)
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

            check_dict = st.session_state.get("check_dict", {})
            details_per_artist = check_dict.get("details_per_artist", {})

            if not details_per_artist:
                # 만약 generate_report 쪽에서 details_per_artist를 기록하지 않았다면,
                # 이 부분이 empty일 수 있으므로 경고
                st.warning("세부 검증용 데이터가 존재하지 않습니다.")
            else:
                import pandas as pd

                # details_per_artist 구조:
                # {
                #   "아티스트A": {"input_전월잔액":..., "input_당월차감액":..., ... "calc_최종정산금액": ...},
                #   "아티스트B": {...},
                #   ...
                # }

                # (1) DataFrame 생성
                df_list = []
                for artist, val_dict in details_per_artist.items():
                    row = {"아티스트": artist}
                    row.update(val_dict)  # val_dict의 key/value를 row에 추가
                    df_list.append(row)

                df = pd.DataFrame(df_list)

                # (2) 화면에 표시
                st.dataframe(df)

                # 필요시 특정 컬럼만 골라 표시할 수도 있음:
                # selected_cols = [
                #     "아티스트",
                #     "input_전월잔액", "input_당월차감액", "input_당월잔액",
                #     "calc_앨범매출합계", "calc_공제적용", "calc_최종정산금액",
                #     "diff_공제_검증", "diff_잔액_검증"
                # ]
                # st.dataframe(df[selected_cols])

    else:
        st.warning("정산 보고서 생성이 완료되면 이 섹션이 표시됩니다.")






def section_three_upload_and_split_excel():
    """
    3) '엑셀(.xlsx)' 파일 업로드 후, 시트별로 분할 & ZIP 다운로드
    (시트 내 서식/병합/스타일도 그대로 유지하는 방법)
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
            # 1) 업로드된 엑셀파일 전체를 BytesIO로 보관
            original_file_data = uploaded_file.read()

            # 2) 한번 로드해서 시트명 리스트(순서 등) 파악
            try:
                # 여기서 한 번만 load_workbook
                wb = openpyxl.load_workbook(io.BytesIO(original_file_data))
            except Exception as e:
                st.error(f"엑셀 파일을 읽는 중 오류가 발생했습니다: {e}")
                return

            # 3) ZIP 버퍼 준비
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                # 원본 엑셀 파일에 들어있는 모든 시트명 반복
                for sheet_name in wb.sheetnames:
                    # (a) Workbook을 다시 로드 (서식 보존 목적)
                    temp_wb = openpyxl.load_workbook(io.BytesIO(original_file_data))

                    # (b) 이 시트(`sheet_name`)를 제외한 나머지 시트는 모두 삭제
                    for s in temp_wb.sheetnames:
                        if s != sheet_name:
                            ws_remove = temp_wb[s]
                            temp_wb.remove(ws_remove)

                    # (c) 이제 temp_wb에는 sheet_name 시트만 남아있음
                    single_buf = io.BytesIO()
                    temp_wb.save(single_buf)
                    single_buf.seek(0)

                    # 시트 이름에 '/', '\\' 등이 있으면 zip 내부에서 문제될 수 있으므로 치환
                    safe_sheet_name = sheet_name.replace("/", "_").replace("\\", "_")
                    # (d) zip에 추가
                    zf.writestr(f"{safe_sheet_name}.xlsx", single_buf.getvalue())

            zip_buf.seek(0)

            st.success("모든 시트를 개별 엑셀 파일로 분할 완료! (서식/병합 유지됨)")
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

def compare_artists(song_artists, revenue_artists):
    """
    input_song cost 아티스트 목록(song_artists) vs
    input_online revenue 아티스트 목록(revenue_artists) 비교.
    반환: {'missing_in_song': [...], 'missing_in_revenue': [...], 'common_count': N ...}
    """
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
    """예: '202412' → (2024, 12). '202401' → (2024,1). 결과데이터 '2024년 12월'도 같은 것으로 간주 가능."""
    m = m.strip()
    if re.match(r'^\d{6}$', m):  # 202412
        yyyy = int(m[:4])
        mm = int(m[4:])
        return (yyyy, mm)
    # 혹은 '2024년 12월' => 정규표현식 파싱
    pat = r'^(\d{4})년\s*(\d{1,2})월$'
    mmatch = re.match(pat, m)
    if mmatch:
        yyyy = int(mmatch.group(1))
        mm = int(mmatch.group(2))
        return (yyyy, mm)
    # 그 외는 원본 그대로
    return m

def almost_equal(a, b, tol=1e-3):
    """숫자 비교용. 소수점/반올림 미세차이를 무시."""
    return abs(a - b) < tol


# -----------------------------------------------------------------------------
# (추가) 시트별 XLSX 다운로드 → Zip
# -----------------------------------------------------------------------------
def download_all_tabs_as_zip(spreadsheet_id: str, creds, sheet_svc, progress_bar=None) -> bytes:
    from google.auth.transport.requests import AuthorizedSession
    session = AuthorizedSession(creds)

    def get_sheet_list(spreadsheet_id):
        meta = sheet_svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        all_sheets = meta["sheets"]

        sheet_list = []
        for s in all_sheets:
            props = s["properties"]
            sid = props["sheetId"]
            stype = props.get("sheetType", "GRID") 
            title = props["title"]

            # 만약 'GRID' 타입이 아니거나, sheetId=0 인 것은 스킵
            if stype != "GRID" or sid == 0:
                print(f"Skipping non-GRID or GID=0 sheet => id={sid}, title={title}, type={stype}")
                continue

            sheet_list.append((sid, title))

        return sheet_list

    def download_sheet_as_xlsx(spreadsheet_id, sheet_id, session, max_retries=3):
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export"
        params = {"format": "xlsx", "gid": str(sheet_id)}

        for attempt in range(max_retries):
            time.sleep(1)  # 시도마다 잠깐 쉼
            try:
                resp = session.get(url, params=params)
                resp.raise_for_status()
                return resp.content
            except req.exceptions.HTTPError as e:
                if e.response.status_code in [429, 500, 503]:
                    sleep_sec = 2 * attempt
                    time.sleep(sleep_sec)
                    continue
                elif e.response.status_code in [403, 404]:
                    # 403, 404도 1~2초 뒤 재시도 해볼 만함
                    time.sleep(1)
                    continue
                else:
                    # 그 외 상태 코드는 그냥 에러
                    raise e
        raise RuntimeError(f"Download failed after {max_retries} attempts (gid={sheet_id})")
    
    # 1) 스프레드시트 탭 목록 가져오기
    tabs = get_sheet_list(spreadsheet_id)
    total = len(tabs)

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for i, (gid, title) in enumerate(tabs):
            # (A) 각 탭 XLSX 다운로드
            content = download_sheet_as_xlsx(spreadsheet_id, gid, session)
            zf.writestr(f"{title}.xlsx", content)

            # (B) 탭 하나 완료 후 약간 쉼
            time.sleep(1)

            # (C) 진행률 갱신 (if progress_bar is not None)
            if progress_bar is not None and total > 0:
                ratio = (i + 1) / total
                progress_bar.progress(ratio)

    zip_buffer.seek(0)
    return zip_buffer.getvalue()


# ========== [3] 보조 유틸 함수들 ===========
def get_next_month_str(ym: str) -> str:
    """'YYYYMM'을 입력받아 다음 달 'YYYYMM'을 반환."""
    year = int(ym[:4])
    month = int(ym[4:])
    month += 1
    if month > 12:
        year += 1
        month = 1
    return f"{year}{month:02d}"

def create_new_spreadsheet(filename: str, folder_id: str, drive_svc, attempt=1, max_attempts=5) -> str:
    # (기존 코드 동일)
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
        # userRateLimitExceeded 등의 경우 재시도 예시
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

    # 누락된 시트
    missing = [t for t in list_of_sheet_titles if t not in existing_titles]
    if not missing:
        print("모든 시트가 이미 존재합니다.")
        return

    # 분할 크기 설정 (예: 30)
    BATCH_SIZE = 30

    # 하나의 batchUpdate "requests"를 모을 리스트
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

        # 만약 BATCH_SIZE(30)에 도달하면 API 호출
        if len(requests_add) >= BATCH_SIZE:
            body = {"requests": requests_add}
            resp = sheet_svc.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()

            total_count += len(resp["replies"])
            print(f"분할 addSheet 완료: {len(resp['replies'])}개 생성")
            requests_add.clear()
            time.sleep(2)  # 잠깐 쉬기 (Rate Limit 완화)

    # 남아있는 요청이 있으면 마무리 전송
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

    # ***해당 코드 추가 확인 필요***
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
    rows_sc = data_sc[1:-1]

    try:
        idx_artist = header_sc.index("아티스트명")
        idx_rate = header_sc.index("정산 요율")
        idx_prev = header_sc.index("전월 잔액")
        idx_deduct = header_sc.index("당월 차감액")
        idx_remain = header_sc.index("당월 잔액")
    except ValueError as e:
        st.error(f"[input_song cost] 시트 컬럼 명이 맞는지 확인 필요: {e}")
        return ""

    def to_num(x):
        if not x: return 0.0
        return float(x.replace("%","").replace(",",""))

    artist_cost_dict = {}
    for row in rows_sc:
        artist = row[idx_artist]
        if not artist: 
            continue
        cost_data = {
            "정산요율": to_num(row[idx_rate]),
            "전월잔액": to_num(row[idx_prev]),
            "당월차감액": to_num(row[idx_deduct]),
            "당월잔액": to_num(row[idx_remain])
        }
        artist_cost_dict[artist] = cost_data

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
        st.error(f"[input_online revenue] 시트 컬럼 명이 맞는지 확인 필요: {e}")
        return ""

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
            rv_val = float(row[col_revenue].replace(",",""))
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
    # [추가] 체크 딕셔너리 안에 details_per_artist 키 준비
    # ---------------------------------------------------------
    if "details_per_artist" not in check_dict:
        check_dict["details_per_artist"] = {}

    # 검증
    song_artists = [r[idx_artist] for r in rows_sc if r[idx_artist]]
    revenue_artists = [r[col_aartist].strip() for r in rows_or if r[col_aartist].strip()]
    check_dict["song_artists"] = song_artists
    check_dict["revenue_artists"] = revenue_artists
    compare_res = compare_artists(song_artists, revenue_artists)
    check_dict["artist_compare_result"] = compare_res

    # ------------------- (C) 아티스트 목록 ---------------
    all_artists = sorted(set(artist_cost_dict.keys())|set(artist_revenue_dict.keys()))
    all_artists = [a for a in all_artists if a and a != "합계"]
    st.session_state["all_artists"] = all_artists

    # ------------------- (D) output_report_YYYYMM --------
    out_filename = f"ouput_report_{ym}"
    out_file_id = create_new_spreadsheet(out_filename, folder_id, drive_svc)
    out_sh = gc.open_by_key(out_file_id)
    
    # sheet1 삭제
    try:
        out_sh.del_worksheet(out_sh.worksheet("Sheet1"))
    except:
        pass

    year_val = ym[:4]
    month_val = ym[4:]

    # (1) placeholder 준비
    artist_placeholder = st.empty()

    # (2) 진행률 바 준비
    progress_bar = st.progress(0)

    # ────────────── (1) batchUpdate를 한 번만 쓰기 위해 requests 모을 리스트 ──────────────
    all_requests = []
    
    needed_titles = []
    for artist in all_artists:
        needed_titles.append(f"{artist}(세부매출내역)")
        needed_titles.append(f"{artist}(정산서)")

    # 3) batch_add_sheets
    batch_add_sheets(out_file_id, sheet_svc, needed_titles)
    # 이때, batch_add_sheets는 위에 예시로 만든 함수

    # --------------- (E) 아티스트별 시트 만들기 -----------
    for i, artist in enumerate(all_artists):
        ratio = (i + 1) / len(all_artists)

        # 진행률 바 업데이트
        progress_bar.progress(ratio)

        # placeholder에 “현재 아티스트” 안내
        artist_placeholder.info(f"[{i+1}/{len(all_artists)}] '{artist}' 처리 중...")

        # (실제 처리 로직 / time.sleep 등)

        # ----------------------------
        # 세부매출내역 탭
        # ----------------------------
        # ws_detail_name = f"{artist}(세부매출내역)"
        # ws_detail = create_worksheet_if_not_exists(out_sh, ws_detail_name, rows=200, cols=7)
        # ws_detail.clear()

        ws_detail = out_sh.worksheet(f"{artist}(세부매출내역)")

        # detail data
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
        detail_matrix.append(["합계","","","","","", to_currency(total_det)])
        row_cursor_detail_end = len(detail_matrix)

        ws_detail.update(
            range_name="A1",
            values=detail_matrix
        )

        time.sleep(3)        

        # build requests:
        detail_requests = []

        # updateSheetProperties 로 resize
        detail_requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws_detail.id,
                    "gridProperties": {
                        "rowCount": row_cursor_detail_end,
                        "columnCount": 7
                    }
                },
                "fields": "gridProperties(rowCount,columnCount)"
            }
        })

        # 1) 열 너비 (A: 120, B:140, ...)
        # updateDimensionProperties -> dimension='COLUMNS', range-> startIndex=0 ...
        # A=0, B=1, ...
        # 예시: A열(0)
        detail_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_detail.id,
                    "dimension": "COLUMNS",
                    "startIndex": 0, 
                    "endIndex": 1  # A열 한 칸
                },
                "properties": {
                    "pixelSize": 140
                },
                "fields": "pixelSize"
            }
        })
        # B열
        detail_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_detail.id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,  # B
                    "endIndex": 2
                },
                "properties": {
                    "pixelSize": 140
                },
                "fields": "pixelSize"
            }
        })
        # E열 -> startIndex=4, endIndex=5
        detail_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_detail.id,
                    "dimension": "COLUMNS",
                    "startIndex": 4,
                    "endIndex": 5
                },
                "properties": {
                    "pixelSize": 120
                },
                "fields": "pixelSize"
            }
        })

        # 2) row 높이: (옵션) 특정 행만 or 전체
        # updateDimensionProperties -> dimension='ROWS'
        #  예) 1행(0-based -> row=0) 높이 40
        #    => startIndex=0, endIndex=1
        # if needed

        # 3) 헤더(A1:G1) 배경/폰트
        #  => "repeatCell": { "range": ..., "cell": { "userEnteredFormat": {...} } }
        #  여기서는 "헤더행" 하나만이므로 row=0..1, col=0..7
        detail_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_detail.id,
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

        # 4) 합계행 병합 & 포매팅
        #    A{row_cursor_detail_end}:F{row_cursor_detail_end}
        # => row_cursor_detail_end-1 (0-based)
        # => ex) if 12행, then row=11
        sum_row_0based = row_cursor_detail_end-1  # 0-based
        # 병합
        detail_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_detail.id,
                    "startRowIndex": sum_row_0based,
                    "endRowIndex": sum_row_0based+1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 6
                },
                "mergeType": "MERGE_ALL"
            }
        })
        # 배경색/가운데 정렬
        detail_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_detail.id,
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
                        "textFormat": {
                            "bold": True
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })
        # 합계값 오른쪽 정렬 => G{sum_row_0based}
        detail_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_detail.id,
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
        # 매출 순수익 칼럼 값 오른쪽 정렬 
        detail_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_detail.id,
                    "startRowIndex": 1,
                    "endRowIndex": sum_row_0based,
                    "startColumnIndex": 6,
                    "endColumnIndex": 7
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "RIGHT",
                        "textFormat": {"bold": False}
                    }
                },
                "fields": "userEnteredFormat(horizontalAlignment,textFormat)"
            }
        })

        # 5) 전체 테두리
        # => A1~G{row_cursor_detail_end}
        # => row=0..row_cursor_detail_end, col=0..7
        detail_requests.append({
            "updateBorders": {
                "range": {
                    "sheetId": ws_detail.id,
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
                "innerVertical": {"style":"SOLID","width":1}
            }
        })

        all_requests.extend(detail_requests)

        # (추가) 분할 batchUpdate 체크
        if len(all_requests) >= 200:  # 예: 80개 정도마다 전송
            sheet_svc.spreadsheets().batchUpdate(
                spreadsheetId=out_file_id,
                body={"requests": all_requests}
            ).execute()
            all_requests.clear()     # 전송 후 비우기
            time.sleep(3)           # 잠시 쉼 (네트워크 안정화)



        # ------------------------------------------------------
        # 정산서 탭 (batchUpdate 방식)
        # ------------------------------------------------------

        ws_report = out_sh.worksheet(f"{artist}(정산서)")
        ws_report_id = ws_report.id
        

        # 1) report_matrix 생성 (기존 방식 그대로)
        report_matrix = []
        for _ in range(300):
            report_matrix.append([""] * 8)

        report_matrix[1][6] = report_date
        report_matrix[3][1] = f"{year_val}년 {month_val}월 판매분"
        report_matrix[5][1] = f"{artist}님 음원 정산 내역서"

        report_matrix[7][0] = "•"
        report_matrix[7][1] = "저희와 함께해 주셔서 정말 감사하고 앞으로도 잘 부탁드리겠습니다!"
        report_matrix[8][0] = "•"
        report_matrix[8][1] = f"{year_val}년 {month_val}월 음원의 수익을 아래와 같이 정산드립니다."
        report_matrix[9][0] = "•"
        report_matrix[9][1] = "정산 관련하여 문의사항이 있다면 무엇이든, 언제든 편히 메일 주세요!"
        report_matrix[9][5] = "E-Mail : lucasdh3013@naver.com"

        # 1. 음원 서비스별
        report_matrix[12][0] = "1."
        report_matrix[12][1] = "음원 서비스별 정산내역"
        header_row1 = 13
        headers_1 = ["앨범", "대분류", "중분류", "서비스명", "기간", "매출액"]
        for i, val in enumerate(headers_1):
            report_matrix[header_row1][1 + i] = val

        row_cursor = header_row1 + 1
        sum_1 = 0
        for d in details_sorted:
            rev = d["revenue"]
            sum_1 += rev
            report_matrix[row_cursor][1] = d["album"]
            report_matrix[row_cursor][2] = d["major"]
            report_matrix[row_cursor][3] = d["middle"]
            report_matrix[row_cursor][4] = d["service"]
            report_matrix[row_cursor][5] = f"{year_val}년 {month_val}월"
            report_matrix[row_cursor][6] = to_currency(rev)
            row_cursor += 1

        row_cursor += 1
        row_cursor_sum1 = row_cursor + 1
        report_matrix[row_cursor][1] = "합계"
        report_matrix[row_cursor][6] = to_currency(sum_1)
        row_cursor += 2

        # 2. 앨범별 정산
        report_matrix[row_cursor][0] = "2."
        report_matrix[row_cursor][1] = "앨범 별 정산 내역"
        row_cursor += 1
        row_cursor_album = row_cursor
        report_matrix[row_cursor][1] = "앨범"
        report_matrix[row_cursor][5] = "기간"
        report_matrix[row_cursor][6] = "매출액"
        row_cursor += 1

        album_sum = defaultdict(float)
        for d in details_sorted:
            album_sum[d["album"]] += d["revenue"]

        sum_2 = 0
        for alb in sorted(album_sum.keys(), key=album_sort_key):
            amt = album_sum[alb]
            sum_2 += amt
            report_matrix[row_cursor][1] = alb
            report_matrix[row_cursor][5] = f"{year_val}년 {month_val}월"
            report_matrix[row_cursor][6] = to_currency(amt)
            row_cursor += 1

        row_cursor_sum2 = row_cursor + 1
        report_matrix[row_cursor][1] = "합계"
        report_matrix[row_cursor][6] = to_currency(sum_2)
        row_cursor += 2

        # 3. 공제 내역
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

        row_cursor_sum3 = row_cursor + 1
        prev_val = artist_cost_dict[artist]["전월잔액"]
        deduct_val = artist_cost_dict[artist]["당월차감액"]
        remain_val = artist_cost_dict[artist]["당월잔액"]
        공제적용 = sum_2 - deduct_val

        alb_list = sorted(album_sum.keys(), key=album_sort_key)
        alb_str = ", ".join(alb_list) if alb_list else "(앨범 없음)"
        report_matrix[row_cursor][1] = alb_str
        report_matrix[row_cursor][2] = to_currency(prev_val)
        report_matrix[row_cursor][3] = to_currency(deduct_val)
        report_matrix[row_cursor][5] = to_currency(remain_val)
        report_matrix[row_cursor][6] = to_currency(공제적용)
        row_cursor += 2

        # 4. 수익 배분
        report_matrix[row_cursor][0] = "4."
        report_matrix[row_cursor][1] = "수익 배분"
        row_cursor += 1
        row_cursor_rate = row_cursor
        report_matrix[row_cursor][1] = "앨범"
        report_matrix[row_cursor][2] = "항목"
        report_matrix[row_cursor][3] = "적용율"
        report_matrix[row_cursor][6] = "적용 금액"
        row_cursor += 1

        rate_val = artist_cost_dict[artist]["정산요율"]
        final_amount = 공제적용 * (rate_val / 100.0)
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

        # (2) 시트에 업데이트
        ws_report.update(
            range_name="A1",
            values=report_matrix)



        # ---------------------------------------------------------
        # 계산된 값들 -> check_dict["details_per_artist"][artist] 에 저장
        # ---------------------------------------------------------
        check_dict["details_per_artist"][artist] = {
            "input_전월잔액": prev_val,
            "input_당월차감액": deduct_val,
            "input_당월잔액": remain_val,
            "input_정산요율(%)": rate_val,

            "calc_앨범매출합계": sum_2,
            "calc_공제적용": 공제적용,
            "calc_최종정산금액": final_amount,

            # 추가로, ex) 차이 비교 예시
            #  - (remain_val + deduct_val)와 (prev_val) 의 차이가 0이면 정상
            #  - sum_2 - deduct_val 과 공제적용 이 같은지 등
            "diff_공제_검증": (sum_2 - deduct_val) - 공제적용,
            "diff_잔액_검증": (prev_val - deduct_val) - remain_val,
        }





        time.sleep(3)   

        # (3) 한 번의 batchUpdate: 열너비, 행높이, 병합, 서식, 테두리 ...
        report_requests = []

        # updateSheetProperties 로 resize
        report_requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": ws_report.id,
                    "gridProperties": {
                        "rowCount": row_cursor_report_end,
                        "columnCount": 8
                    }
                },
                "fields": "gridProperties(rowCount,columnCount)"
            }
        })

        # 3-1) 열너비 (A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7)
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 1
                },
                "properties": { "pixelSize": 40 },
                "fields": "pixelSize"
            }
        })
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "COLUMNS",
                    "startIndex": 1,
                    "endIndex": 2
                },
                "properties": { "pixelSize": 200 },
                "fields": "pixelSize"
            }
        })
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "COLUMNS",
                    "startIndex": 2,
                    "endIndex": 3
                },
                "properties": { "pixelSize": 130 },
                "fields": "pixelSize"
            }
        })
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "COLUMNS",
                    "startIndex": 3,
                    "endIndex": 4
                },
                "properties": { "pixelSize": 120 },
                "fields": "pixelSize"
            }
        })
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "COLUMNS",
                    "startIndex": 4,
                    "endIndex": 5
                },
                "properties": { "pixelSize": 130 },
                "fields": "pixelSize"
            }
        })
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "COLUMNS",
                    "startIndex": 5,
                    "endIndex": 6
                },
                "properties": { "pixelSize": 130 },
                "fields": "pixelSize"
            }
        })
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "COLUMNS",
                    "startIndex": 6,
                    "endIndex": 7
                },
                "properties": { "pixelSize": 130 },
                "fields": "pixelSize"
            }
        })
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "COLUMNS",
                    "startIndex": 7,
                    "endIndex": 8
                },
                "properties": { "pixelSize": 40 },
                "fields": "pixelSize"
            }
        })

        # 3-2) 행높이 (옵션) => 예) 4행(3 in 0-based), 6행(5 in 0-based) 높이=30
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "ROWS",
                    "startIndex": 3,  # 4행
                    "endIndex": 4
                },
                "properties": { "pixelSize": 30 },
                "fields": "pixelSize"
            }
        })
        report_requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": ws_report_id,
                    "dimension": "ROWS",
                    "startIndex": 5,  # 6행
                    "endIndex": 6
                },
                "properties": { "pixelSize": 30 },
                "fields": "pixelSize"
            }
        })


        # 4-1) 상단 고정 항목(발행 날짜, H2: row=1, col=6)
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

        # 4-2) 상단 고정 항목(판매분, B4:E4)
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

        # 4-3) 상단 고정 항목(아티스트 정산내역서, B6:G6)
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

        # 4-4) 상단 고정 항목(안내문, B8:E8~B10:E10)
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
        
        # 4-5) 1열 정렬 (번호 영역)
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

        # 4-6) 하단 고정 항목(부가세, G)
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
    

        # 5-1) "음원 서비스별 정산내역" 표 타이틀
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
        # 5-2) "음원 서비스별 정산내역" 표 헤더 (Row=13)
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
        # 5-3) 합계행 전 병합
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
        # 5-4) 합계행 병합
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
        # 5-5) 표에 Banding (줄무늬 효과)
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


        # 6-1) 앨범별 정산내역 타이틀
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_album-1,
                    "endRowIndex": row_cursor_album,
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

        # 6-2) 앨범별 정산내역 헤더
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_album,
                    "endRowIndex": row_cursor_album+1,
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
        # 6-3) 앨범별 정산내역 표 본문
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_album+1,
                    "endRowIndex": row_cursor_sum2-1,
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
        # 6-4) 앨범별 정산내역 합계행
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum2-1,
                    "endRowIndex": row_cursor_sum2,
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
        # 6-5) 합계행 병합
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum2-1,
                    "endRowIndex": row_cursor_sum2,
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
                    "startRowIndex": row_cursor_sum2-1,
                    "endRowIndex": row_cursor_sum2,
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
                    "startRowIndex": row_cursor_sum2-1,
                    "endRowIndex": row_cursor_sum2,
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


        # 7-1) 공제 내역 타이틀
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_deduction-1,  # (4-1)
                    "endRowIndex": row_cursor_deduction,
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

        # 7-2) 공제 내역 헤더
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_deduction,
                    "endRowIndex": row_cursor_deduction+1,
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
        
        # 7-3) 공제 내역 표 본문 (데이터부분)
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_deduction+1,
                    "endRowIndex": row_cursor_deduction+2,
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
        
        # 7-4) 공제 내역 표 본문 (합계 부분)
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_deduction+1,
                    "endRowIndex": row_cursor_deduction+2,
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


        # 8-1) 수익 배분 타이틀
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_rate-1,
                    "endRowIndex": row_cursor_rate,
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

        # 8-2) 수익 배분 헤더
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
        
        # 8-3) 수익 배분 표 본문 
        report_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_rate+1,
                    "endRowIndex": row_cursor_rate+2,
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
        
        # 8-4) 수익 배분 표 합계행 병합
        report_requests.append({
            "mergeCells": {
                "range": {
                    "sheetId": ws_report_id,
                    "startRowIndex": row_cursor_sum4,
                    "endRowIndex": row_cursor_sum4+1,
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
                    "startRowIndex": row_cursor_sum4,
                    "endRowIndex": row_cursor_sum4+1,
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
                    "startRowIndex": row_cursor_sum4,
                    "endRowIndex": row_cursor_sum4+1,
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


        # 9-1) 전체 테두리 화이트
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
        
        # 9-2) 표 부분 점선 
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
        add_dotted_borders(row_cursor_deduction, row_cursor_sum3, 1, 7)
        # 4번 섹션 
        add_dotted_borders(row_cursor_rate, row_cursor_sum4+1, 1, 7)
        
        # 9-3) 시트 외곽 검정 SOLID 
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

        # (추가) 분할 batchUpdate 체크
        if len(all_requests) >200:
            sheet_svc.spreadsheets().batchUpdate(
                spreadsheetId=out_file_id,
                body={"requests": all_requests}
            ).execute()
            all_requests.clear()
            time.sleep(3)


    # -----------------
    # batchUpdate 실행
    # -----------------
    if all_requests:
        sheet_svc.spreadsheets().batchUpdate(
            spreadsheetId=out_file_id,
            body={"requests": all_requests}
        ).execute()
        all_requests.clear()

    time.sleep(2)

    # 루프 끝나면 처리 완료 메시지 (원한다면)
    artist_placeholder.success("모든 아티스트 처리 완료!")

    # 다음 달 탭 복제
    next_ym = get_next_month_str(ym)
    new_ws = duplicate_worksheet_with_new_name(song_cost_sh, ym, next_ym)
    new_data = new_ws.get_all_values()
    if not new_data:
        st.warning(f"'{ym}' → '{next_ym}' 탭 복제했는데 비어있음.")
    else:
        hdr = new_data[0]
        try:
            idxp = hdr.index("전월 잔액")
            idxc = hdr.index("당월 잔액")
        except ValueError:
            st.warning("전월 잔액/당월 잔액 칼럼 없음")
            return out_file_id
        content = new_data[1:]
        updated = []
        for row in content:
            row_data = row[:]
            try:
                cur_val = float(row_data[idxc]) if row_data[idxc] else 0.0
            except:
                cur_val = 0.0
            row_data[idxp] = str(cur_val)
            updated.append(row_data)
        if updated:
            new_ws.update(
                range_name="A2",
                values=updated,
                value_input_option="USER_ENTERED"
            )
        time.sleep(1)   
    return out_file_id

# ========== [5] Streamlit UI =============
def main():
    st.title("아티스트 음원 정산 보고서 자동 생성기")

    # 2) 섹션 1: 보고서 생성
    section_one_report_input()
    st.divider()

    # 3) 섹션 2: 시트 링크 및 검증 결과
    section_two_sheet_link_and_verification()
    st.divider()

    # 4) 섹션 3: 압축파일 다운로드
    section_three_upload_and_split_excel()

if __name__ == "__main__":
    main()
