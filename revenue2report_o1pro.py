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

# 추가: AuthorizedSession (시트별 XLSX 다운로드 시 사용)
from google.auth.transport.requests import AuthorizedSession
from collections import defaultdict


# ========== [1] 인증/초기설정 =============
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

def get_credentials_from_secrets():
    """
    Streamlit secrets에 저장된 google_service_account 정보를 이용해
    Credentials 객체를 생성하는 헬퍼 함수.
    """
    service_account_info = st.secrets["google_service_account"]
    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=SCOPES
    )
    return credentials


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

def download_all_tabs_as_zip(spreadsheet_id: str, creds, sheet_svc) -> bytes:
    # 기존 코드와 동일하게, 모든 탭 XLSX를 zip으로 묶어 반환
    from google.auth.transport.requests import AuthorizedSession
    session = AuthorizedSession(creds)
    def get_sheet_list(spreadsheet_id):
        meta = sheet_svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = meta["sheets"]
        return [(s["properties"]["sheetId"], s["properties"]["title"]) for s in sheets]

    def download_sheet_as_xlsx(spreadsheet_id, sheet_id, session):
        url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export"
        params = {"format": "xlsx", "gid": str(sheet_id)}
        resp = session.get(url, params=params)
        resp.raise_for_status()
        return resp.content

    tabs = get_sheet_list(spreadsheet_id)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for (gid, title) in tabs:
            # (1) 다운로드
            content = None
            for attempt in range(3):  # 재시도 3번 예시
                try:
                    content = download_sheet_as_xlsx(spreadsheet_id, gid, session)
                    break
                except req.exceptions.HTTPError as e:
                    # 429, 503 등 일시적 에러면 time.sleep 후 재시도
                    if e.response.status_code in (429, 503):
                        sleep_sec = 2 ** attempt  # 지수 백오프 예
                        time.sleep(sleep_sec)
                    else:
                        raise e
            
            if content is None:
                raise RuntimeError(f"Download failed after retries (gid={gid}, title={title})")

            # (2) Zip에 추가
            zf.writestr(f"{title}.xlsx", content)

            # (3) 루프간 sleep
            time.sleep(2)  # 시트마다 0.5~1초 대기


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

def create_worksheet_if_not_exists(gs_obj: gspread.Spreadsheet, sheet_name: str, rows=200, cols=8):
    """
    시트가 이미 존재하면 재사용, 없으면 새로 생성.
    """
    all_ws = gs_obj.worksheets()
    for w in all_ws:
        if w.title == sheet_name:
            return w
    return gs_obj.add_worksheet(title=sheet_name, rows=rows, cols=cols)


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
    sheet_svc
):
    folder_id = st.secrets["google_service_account"]["folder_id"]

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

    # ------------------- (D) output_report_YYYYMM --------
    out_filename = f"ouput_report_{ym}"
    out_file_id = create_new_spreadsheet(out_filename, folder_id, drive_svc)
    out_sh = gc.open_by_key(out_file_id)
    
    time.sleep(2)
    # sheet1 삭제
    try:
        out_sh.del_worksheet(out_sh.sheet1)
    except:
        pass

    year_val = ym[:4]
    month_val = ym[4:]

    # 진행률
    progress_bar = st.progress(0)
    num_art = len(all_artists)

    # --------------- (E) 아티스트별 시트 만들기 -----------
    for i, artist in enumerate(all_artists):
        pr = (i+1)/num_art
        progress_bar.progress(pr)
        st.info(f"[{i+1}/{num_art}] 현재 처리중: '{artist}'")
        time.sleep(4)

        # ----------------------------
        # 세부매출내역 탭
        # ----------------------------
        ws_detail_name = f"{artist}(세부매출내역)"
        ws_detail = create_worksheet_if_not_exists(out_sh, ws_detail_name, rows=200, cols=7)
        ws_detail.clear()

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
        ws_detail.resize(rows=row_cursor_detail_end, cols=7)
        
        time.sleep(1)

        # build requests:
        requests = []

        # 1) 열 너비 (A: 120, B:140, ...)
        # updateDimensionProperties -> dimension='COLUMNS', range-> startIndex=0 ...
        # A=0, B=1, ...
        # 예시: A열(0)
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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

        # 매출 칼럼 오른쪽 정렬
        fmt_right = CellFormat(horizontalAlignment="RIGHT")
        format_cell_range(ws_detail, f"G2:G{row_cursor_detail_end}", fmt_right)

        # 5) 전체 테두리
        # => A1~G{row_cursor_detail_end}
        # => row=0..row_cursor_detail_end, col=0..7
        requests.append({
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

        if requests:
            sheet_svc.spreadsheets().batchUpdate(
                spreadsheetId=out_file_id,
                body={"requests": requests}
            ).execute()
        
        time.sleep(2)


        # ---------------------------
        # 정산서 탭 (batchUpdate 방식)
        # ---------------------------

        ws_report_name = f"{artist}(정산서)"
        ws_report = create_worksheet_if_not_exists(out_sh, ws_report_name, rows=200, cols=8)
        ws_report_id = ws_report.id
        ws_report.clear()

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
        ws_report.resize(rows=row_cursor_report_end, cols=8)

        time.sleep(1)   

        # (3) 한 번의 batchUpdate: 열너비, 행높이, 병합, 서식, 테두리 ...
        requests = []

        # 3-1) 열너비 (A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7)
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
            requests.append({
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
            requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
        requests.append({
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
            requests.append({
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
        requests.append({
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
            
        # -----------------
        # batchUpdate 실행
        # -----------------
        if requests:
            sheet_svc.spreadsheets().batchUpdate(
                spreadsheetId=out_file_id,
                body={"requests": requests}
            ).execute()

        time.sleep(2)

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

    credentials = get_credentials_from_secrets()
    gc_local = gspread.authorize(credentials)
    drive_service_local = build("drive","v3",credentials=credentials)
    sheet_service_local = build("sheets","v4",credentials=credentials)

    check_dict = {
        "song_artists": [],
        "revenue_artists": [],
        "artist_compare_result": {}
    }

    ym = st.text_input("진행기간(YYYYMM)", "")
    report_date = st.text_input("보고서 발행 날짜 (YYYY-MM-DD)", "")

    if st.button("작업 시작하기"):
        if not ym or not re.match(r'^\d{6}$', ym):
            st.error("진행기간 6자리를 입력하세요.")
            return
        if not report_date:
            report_date = str(datetime.date.today())

        out_file_id = generate_report(
            ym, report_date, check_dict,
            gc=gc_local,
            drive_svc=drive_service_local,
            sheet_svc=sheet_service_local
        )
        if out_file_id:
            st.success(f"'{ym}' 보고서 생성 완료 (ID={out_file_id})")
            st.info(f"https://docs.google.com/spreadsheets/d/{out_file_id}/edit")

            # XLSX(zip) 다운로드
            zip_data = download_all_tabs_as_zip(out_file_id, credentials, sheet_service_local)
            st.download_button("XLSX(zip) 다운로드", data=zip_data, file_name=f"{ym}_report.zip", mime="application/zip")

            # 검증결과
            def show_verification_result(check_dict):
                st.subheader("검증 결과")
                ar = check_dict.get("artist_compare_result", {})
                if ar:
                    st.write(f"- Song cost 아티스트 수 = {ar['song_count']}")
                    st.write(f"- Revenue 아티스트 수 = {ar['revenue_count']}")
                    st.write(f"- 공통 아티스트 수 = {ar['common_count']}")
                    if ar["missing_in_song"]:
                        st.warning(f"Song에 없고 Revenue에만 있는: {ar['missing_in_song']}")
                    if ar["missing_in_revenue"]:
                        st.warning(f"Revenue에 없고 Song에만 있는: {ar['missing_in_revenue']}")
                else:
                    st.write("원본 비교 결과 없음")

            show_verification_result(check_dict)

if __name__ == "__main__":
    main()
