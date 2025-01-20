import streamlit as st
import datetime
import re
import time
import io
import zipfile

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
SERVICE_ACCOUNT_FILE = "report-revenue-448317-0d6948a0250a.json"  # 본인 환경에 맞게 수정
FOLDER_ID = "19lpAoCxQBzMKiLikOwfX1GSw1vWk_4qr"  # 본인 폴더 ID

credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(credentials)

drive_service = build("drive", "v3", credentials=credentials)
sheet_service = build("sheets", "v4", credentials=credentials)


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

def get_sheet_list(spreadsheet_id, creds):
    """스프레드시트의 모든 탭(sheetId, title) 목록을 가져온다."""
    meta = sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta["sheets"]
    tab_list = []
    for s in sheets:
        sid = s["properties"]["sheetId"]  # gid
        title = s["properties"]["title"]
        tab_list.append((sid, title))
    return tab_list

def download_sheet_as_xlsx(spreadsheet_id: str, sheet_id: int, session: AuthorizedSession) -> bytes:
    """
    특정 탭(gid=sheet_id)을 XLSX로 다운로드해서 bytes로 반환.
    """
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export"
    params = {
        "format": "xlsx",
        "gid": str(sheet_id)
    }
    resp = session.get(url, params=params)
    resp.raise_for_status()
    return resp.content  # XLSX in bytes

def download_all_tabs_as_zip(spreadsheet_id: str, creds) -> bytes:
    """
    스프레드시트 ID내 모든 탭을 각각 XLSX로 다운로드한 뒤, zip 형태(bytes)로 묶어서 반환
    """
    # (1) 모든 탭 목록
    tabs = get_sheet_list(spreadsheet_id, creds)
    # AuthorizedSession
    session = AuthorizedSession(creds)

    # (2) in-memory zip
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for (gid, title) in tabs:
            content = download_sheet_as_xlsx(spreadsheet_id, gid, session)
            # 파일명: "{탭제목}.xlsx"
            xlsx_filename = f"{title}.xlsx"
            zf.writestr(xlsx_filename, content)

    zip_buffer.seek(0)
    return zip_buffer.getvalue()

# ========== [2] 포매팅 함수 (batchUpdate) ===========
def apply_section_styles(
    sheet_id: int, 
    spreadsheet_id: str, 
    row_cursor_album: int, 
    row_cursor_deduction: int, 
    row_cursor_rate: int, 
    row_cursor_report_end: int, 
    row_cursor_sum1: int, 
    row_cursor_sum2: int, 
    row_cursor_sum3: int, 
    row_cursor_sum4: int
):
    # =================================
    # [1] 기존 banding(줄무늬) 모두 삭제
    # =================================
    sheet_data = sheet_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=[],
        includeGridData=False
    ).execute()

    delete_requests = []

    # "sheet_data['sheets']"에 시트별 정보가 들어있음
    # sheet_id와 일치하는 시트에서, bandedRanges가 있으면 전부 제거
    for sht in sheet_data["sheets"]:
        if sht["properties"]["sheetId"] == sheet_id:
            if "bandedRanges" in sht:
                for br in sht["bandedRanges"]:
                    delete_requests.append({
                        "deleteBanding": {
                            "bandedRangeId": br["bandedRangeId"]
                        }
                    })

    # 삭제 요청이 있다면, batchUpdate
    if delete_requests:
        sheet_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": delete_requests}
        ).execute()


    """
    보고서(정산서) 탭 내 성공적으로 스타일(폰트, 배경색 등) 적용 완료.
    """
    requests = []

    # (예시) 보고서 발행 날짜 (H2: row=1, col=6)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })

    # YYYY년 MM월 판매분 (C4 → row=3, col=1)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 3,
                "endRowIndex": 4,
                "startColumnIndex": 1,
                "endColumnIndex": 2
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
            "fields": "userEnteredFormat"
        }
    })

    # 아티스트님 음원 정산 내역서 (C6 병합 + 가운데 정렬, 폰트 15)
    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
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
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })

    # 안내문 (C8,9,10 → row=7..9, col=0..7)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 7,
                "endRowIndex": 10,
                "startColumnIndex": 0,
                "endColumnIndex": 7
            },
            "cell": {
                "userEnteredFormat": {
                    "horizontalAlignment": "LEFT",
                    "verticalAlignment": "MIDDLE",
                    "textFormat": {
                        "fontFamily": "Malgun Gothic",
                        "fontSize": 10,
                        "bold": False
                    }
                }
            },
            "fields": "userEnteredFormat"
        }
    })

    # 1열 정렬 (번호 영역)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })

    # E-Mail 칸(G10 → row=9, col=6)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })

    # "음원 서비스별 정산내역" 표 헤더 (Row=13)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    # 합계행 전 병합
    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": row_cursor_sum1-2,
                "endRowIndex": row_cursor_sum1-1,
                "startColumnIndex": 1,
                "endColumnIndex": 7
            },
            "mergeType": "MERGE_ALL"
        }
    })
    # 합계행 병합
    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
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
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })

    # 표에 Banding (줄무늬 효과) 예시
    banding_start_row = 14
    banding_end_row = row_cursor_sum1 - 2
    banding_start_col = 1
    banding_end_col = 7
    if banding_end_row > banding_start_row:  # 유효범위 체크
        requests.append({
            "addBanding": {
                "bandedRange": {
                    "range": {
                        "sheetId": sheet_id,
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
                    "sheetId": sheet_id,
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
                "fields": "userEnteredFormat"
            }
        })

    # 2. 앨범별 정산내역
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    # 앨범별 정산내역 표 본문
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    # 앨범별 정산내역 합계행
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    # 합계행 병합
    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
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
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })

    # 3. 공제 내역
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    # 3.공제내역 표 본문 (데이터부분)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    # 3.공제내역 표 본문 (합계 부분)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })

    # 4. 수익 배분
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    # 4. 수익 배분 표 본문 
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    # 4. 수익 배분 표 합계행 병합
    requests.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id,
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
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
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
            "fields": "userEnteredFormat"
        }
    })

    # -------------------------
    # [추가] 테두리 설정 부분
    # -------------------------
    black = {"red": 0, "green": 0, "blue": 0}
    white = {"red": 1, "green": 1, "blue": 1}

    # (A) A1:H48 전체 테두리 전부 NONE으로 초기화
    #     => 행=0..48, 열=0..8 (endRowIndex, endColumnIndex는 '미포함')
    requests.append({
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": row_cursor_report_end,  # A1~A48 => 48행
                "startColumnIndex": 0,
                "endColumnIndex": 8  # H=7 -> end=8
            },
            "top":    {"style": "SOLID", "width": 1, "color": white},
            "bottom": {"style": "SOLID", "width": 1, "color": white},
            "left":   {"style": "SOLID", "width": 1, "color": white},
            "right":  {"style": "SOLID", "width": 1, "color": white},
            "innerHorizontal": {"style": "SOLID", "width": 1, "color": white},
            "innerVertical":   {"style": "SOLID", "width": 1, "color": white},
        }
    })

    # (B) 1~4 섹션 범위 -> 검정 점선(DOTTED) (예) 1번 섹션: A14:G30
    #    실제 행/열은 사용자 필요에 맞게 조정
    def add_dotted_borders(r1, r2, c1, c2):
        """바깥+안쪽 모두 DOTTED"""
        requests.append({
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": r1,
                    "endRowIndex": r2,
                    "startColumnIndex": c1,
                    "endColumnIndex": c2
                },
                "top":    {"style": "DOTTED", "width": 1, "color": black},
                "bottom": {"style": "DOTTED", "width": 1, "color": black},
                "left":   {"style": "DOTTED", "width": 1, "color": black},
                "right":  {"style": "DOTTED", "width": 1, "color": black},
                "innerHorizontal": {"style": "DOTTED", "width": 1, "color": black},
                "innerVertical":   {"style": "DOTTED", "width": 1, "color": black},
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
    

    # (C) 시트 전체(A1:H48) 바깥 4변만 검정 SOLID 덮어쓰기
    #     => 외곽은 굵은 실선, 내부선은 기존 값 그대로(점선 or 없음)
    requests.append({
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": row_cursor_report_end,
                "startColumnIndex": 0,
                "endColumnIndex": 8
            },
            "top":    {"style": "SOLID", "width": 1, "color": black},
            "bottom": {"style": "SOLID", "width": 1, "color": black},
            "left":   {"style": "SOLID", "width": 1, "color": black},
            "right":  {"style": "SOLID", "width": 1, "color": black}
            # innerHorizontal/innerVertical 생략 => 기존 값 유지
        }
    })

    # batchUpdate 수행
    if requests:
        sheet_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()


def apply_black_borders_to_worksheet(spreadsheet_id, sheet_id, max_rows, max_cols):
    """
    sheet_id에 대해 A1~(max_rows x max_cols) 범위를
    전부 검은색 실선 테두리로 지정.
    """
    black = {"red": 0, "green": 0, "blue": 0}
    requests = [{
        "updateBorders": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": max_rows,
                "startColumnIndex": 0,
                "endColumnIndex": max_cols
            },
            "top":    {"style": "SOLID", "width": 1, "color": black},
            "bottom": {"style": "SOLID", "width": 1, "color": black},
            "left":   {"style": "SOLID", "width": 1, "color": black},
            "right":  {"style": "SOLID", "width": 1, "color": black},
            "innerHorizontal": {"style": "SOLID", "width": 1, "color": black},
            "innerVertical":   {"style": "SOLID", "width": 1, "color": black},
        }
    }]

    sheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()

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

def create_new_spreadsheet(filename: str, folder_id: str, attempt=1, max_attempts=5) -> str:
    # (기존 코드 동일)
    try:
        query = (
            f"parents in '{folder_id}' and trashed=false "
            f"and name='{filename}'"
        )
        response = drive_service.files().list(
            q=query,
            fields="files(id, name)",
            pageSize=50
        ).execute()
        files = response.get("files", [])
        if files:
            existing_file_id = files[0]["id"]
            print(f"파일 '{filename}' 이미 있음 -> 재사용 (ID={existing_file_id})")
            return existing_file_id

        file_metadata = {
            "name": filename,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [folder_id],
        }
        file = drive_service.files().create(body=file_metadata).execute()
        return file["id"]

    except HttpError as e:
        if (e.resp.status == 403 and
            "userRateLimitExceeded" in str(e) and
            attempt < max_attempts):
            sleep_sec = 2 ** attempt
            print(f"[WARN] userRateLimitExceeded -> {sleep_sec}초 후 재시도 (재시도 {attempt}/{max_attempts})")
            time.sleep(sleep_sec)
            return create_new_spreadsheet(filename, folder_id, attempt=attempt+1, max_attempts=max_attempts)
        else:
            raise e

def create_worksheet_if_not_exists(gs: gspread.Spreadsheet, sheet_name: str, rows=200, cols=8):
    # (기존 코드 동일)
    all_ws = gs.worksheets()
    for w in all_ws:
        if w.title == sheet_name:
            return w
    return gs.add_worksheet(title=sheet_name, rows=rows, cols=cols)

def duplicate_worksheet_with_new_name(gs: gspread.Spreadsheet, from_sheet_name: str, to_sheet_name: str):
    # (기존 코드 동일)
    all_ws = gs.worksheets()
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

    new_ws = gs.duplicate_sheet(
        source_sheet_id=from_ws.id,
        new_sheet_name=to_sheet_name
    )
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
def generate_report(ym: str, report_date: str, check_dict: dict) -> str:
    """
    메인 로직:
      1) input_song cost / input_online revenue에서 ym 탭 데이터 읽기
      2) 아티스트별 (세부매출내역), (정산서) 탭 생성 & 작성
      3) 다음달 탭 복제
      4) check_dict를 통해 각 단계에서 검증 결과 누적
    => 최종 생성된 spreadsheetId를 리턴 (다운로드 위해)
    """
    # check_dict: {
    #   'song_artists': [], 'revenue_artists': [],
    #   'artist_mismatch': None,
    #   ... etc ...
    # }

    try:
        song_cost_sh = gc.open("input_song cost")
    except gspread.exceptions.SpreadsheetNotFound:
        st.error("Google Sheet 'input_song cost'를 찾을 수 없습니다.")
        return ""

    song_cost_ws_map = {ws.title: ws for ws in song_cost_sh.worksheets()}
    if ym not in song_cost_ws_map:
        st.error(f"input_song cost에 '{ym}' 탭이 없습니다.")
        return ""

    ws_sc = song_cost_ws_map[ym]
    data_sc = ws_sc.get_all_values()
    if not data_sc:
        st.error(f"input_song cost의 '{ym}' 탭이 비어있습니다.")
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
        if not x:
            return 0.0
        return float(x.replace("%", "").replace(",", ""))

    artist_cost_dict = {}
    for row in rows_sc:
        artist = row[idx_artist]
        if not artist:
            continue
        rate_val = to_num(row[idx_rate])
        prev_val = to_num(row[idx_prev])
        deduct_val = to_num(row[idx_deduct])
        remain_val = to_num(row[idx_remain])
        artist_cost_dict[artist] = {
            "정산요율": rate_val,
            "전월잔액": prev_val,
            "당월차감액": deduct_val,
            "당월잔액": remain_val,
        }

    # input_online revenue
    try:
        online_revenue_sh = gc.open("input_online revenue")
    except gspread.exceptions.SpreadsheetNotFound:
        st.error("Google Sheet 'input_online revenue'를 찾을 수 없습니다.")
        return ""

    online_ws_map = {ws.title: ws for ws in online_revenue_sh.worksheets()}
    if ym not in online_ws_map:
        st.error(f"input_online revenue에 '{ym}' 탭이 없습니다.")
        return ""

    ws_or = online_ws_map[ym]
    data_or = ws_or.get_all_values()
    if not data_or:
        st.error(f"input_online revenue의 '{ym}' 탭이 비어있습니다.")
        return ""

    header_or = data_or[0]
    rows_or = data_or[1:]
    try:
        col_aartist = header_or.index("앨범아티스트")
        col_album = header_or.index("앨범명")
        col_major = header_or.index("대분류")
        col_middle = header_or.index("중분류")
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
            rev_val = float(row[col_revenue].replace(",", ""))
        except:
            rev_val = 0.0
        artist_revenue_dict[a].append({
            "album": alb,
            "major": maj,
            "middle": mid,
            "service": srv,
            "revenue": rev_val
        })

    # 아티스트 검증을 위한 코드
    song_artists = [row[idx_artist] for row in rows_sc if row[idx_artist]]
    revenue_artists = [row[col_aartist].strip() for row in rows_or if row[col_aartist].strip()]

    check_dict["song_artists"] = song_artists
    check_dict["revenue_artists"] = revenue_artists

    res = compare_artists(song_artists, revenue_artists)
    check_dict["artist_compare_result"] = res

    # 아티스트 목록
    all_artists = sorted(set(artist_cost_dict.keys()) | set(artist_revenue_dict.keys()))
    # '합계' 등 제거
    all_artists = [a for a in all_artists if a and a != "합계"]

    # output_report
    output_filename = f"ouput_report_{ym}"
    out_file_id = create_new_spreadsheet(output_filename, FOLDER_ID)
    out_sh = gc.open_by_key(out_file_id)

    # sheet1 삭제
    try:
        out_sh.del_worksheet(out_sh.sheet1)
    except:
        pass

    year_val = ym[:4]
    month_val = ym[4:]

    # [추가] 진행률 바 생성 (Streamlit)
    progress_bar = st.progress(0)
    num_artists = len(all_artists)

    # ----------------------------
    # 아티스트별 시트 생성 루프 (하나만)
    # ----------------------------
    for i, artist in enumerate(all_artists):
        progress_ratio = (i+1) / num_artists
        progress_bar.progress(progress_ratio)

        st.info(f"[{i+1}/{num_artists}] 현재 처리중: '{artist}'")
        time.sleep(0.5)  # (옵션) 속도 제한용

        # 1) 세부매출내역 탭
        ws_detail_name = f"{artist}(세부매출내역)"
        ws_detail = create_worksheet_if_not_exists(out_sh, ws_detail_name, rows=200, cols=7)
        ws_detail.clear()

        detail_header = ["앨범아티스트", "앨범명", "대분류", "중분류", "서비스명", "기간", "매출 순수익"]
        details = artist_revenue_dict.get(artist, [])
        details_sorted = sorted(details, key=lambda d: album_sort_key(d["album"]))

        detail_matrix = []
        detail_matrix.append(detail_header)

        total_detail = 0
        for d in details_sorted:
            rev = d["revenue"]
            total_detail += rev
            detail_matrix.append([
                artist,
                d["album"],
                d["major"],
                d["middle"],
                d["service"],
                f"{year_val}년 {month_val}월",
                to_currency(rev)
            ])

        # 합계행
        detail_matrix.append(["합계", "", "", "", "", "", to_currency(total_detail)])

        # 시트에 업데이트
        ws_detail.update("A1", detail_matrix)
        row_cursor_detail_end = len(detail_matrix)
        ws_detail.resize(rows=row_cursor_detail_end, cols=7)

        # 헤더 포매팅
        fmt_header = CellFormat(
            backgroundColor=Color(1.0, 0.8, 0.008),
            horizontalAlignment="CENTER",
            verticalAlignment="MIDDLE",
            textFormat=TextFormat(bold=True, foregroundColor=Color(0, 0, 0))
        )
        format_cell_range(ws_detail, "A1:G1", fmt_header)
        set_column_width(ws_detail, 'A', 120)
        set_column_width(ws_detail, 'B', 140)
        set_column_width(ws_detail, 'E', 120)

        # 합계행 병합, 배경색, 정렬
        ws_detail.merge_cells(f"A{row_cursor_detail_end}:F{row_cursor_detail_end}")
        fmt_sum = CellFormat(
            backgroundColor=Color(1.0, 0.8, 0.008),
            horizontalAlignment="CENTER",
            verticalAlignment="MIDDLE",
            textFormat=TextFormat(bold=True)
        )
        format_cell_range(ws_detail, f"A{row_cursor_detail_end}:F{row_cursor_detail_end}", fmt_sum)
        # 합계값 오른쪽정렬
        fmt_right_bold = CellFormat(horizontalAlignment="RIGHT", textFormat=TextFormat(bold=True))
        format_cell_range(ws_detail, f"G{row_cursor_detail_end}", fmt_right_bold)

        # 매출 칼럼 오른쪽 정렬
        fmt_right = CellFormat(horizontalAlignment="RIGHT")
        format_cell_range(ws_detail, f"G2:G{row_cursor_detail_end}", fmt_right)

        # 테두리
        sheet_id = ws_detail.id
        apply_black_borders_to_worksheet(out_file_id, ws_detail.id, row_cursor_detail_end, 7)


        # ---------------------------
        # 정산서 탭
        # ---------------------------
        ws_report_name = f"{artist}(정산서)"
        ws_report = create_worksheet_if_not_exists(out_sh, ws_report_name, rows=200, cols=8)
        ws_report_id = ws_report.id
        ws_report.clear()

        # 앨범별 총합 sum_2 구하기
        album_sum = defaultdict(float)
        for d in details_sorted:
            album_sum[d["album"]] += d["revenue"]
        sum_2 = sum(album_sum.values())

        # Song cost 시트에서 차감액/잔액/정산요율
        deduct_val = artist_cost_dict[artist]["당월차감액"]
        remain_val = artist_cost_dict[artist]["당월잔액"]
        rate_val   = artist_cost_dict[artist]["정산요율"]

        # [중요] 공제적용 먼저 계산
        공제적용 = sum_2 - deduct_val

        # 그 다음에야 final_val_in_report 사용 가능
        actual_deduct = int(round(deduct_val))  # 원본
        final_val_in_report = int(round(공제적용))  # 결과
        
        if actual_deduct != final_val_in_report:
            check_dict.setdefault("mismatch_cost", {})
            check_dict["mismatch_cost"].setdefault(artist, []).append(
                f"공제 금액 불일치: 원본={actual_deduct}, 결과={final_val_in_report}"
            )


        set_column_width(ws_report, 'A', 40)
        set_column_width(ws_report, 'B', 200)
        set_column_width(ws_report, 'C', 130)
        set_column_width(ws_report, 'D', 120)
        set_column_width(ws_report, 'E', 130)
        set_column_width(ws_report, 'F', 130)
        set_column_width(ws_report, 'G', 130)
        set_column_width(ws_report, 'H', 40)

        set_row_height(ws_report, "4", 30)
        set_row_height(ws_report, "6", 30)


        report_matrix = []
        for _ in range(300):
            report_matrix.append([""] * 8)

        # 보고서 상단 안내
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

        # 2. 앨범 별 정산
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

        ws_report.update("A1", report_matrix)
        ws_report.resize(rows=row_cursor_report_end, cols=8)


        # 상단 판매분 텍스트 병합 (2024년 12월 판매분)
        ws_report.merge_cells(f"B{4}:E{4}")
        fmt_top_text = CellFormat(
            horizontalAlignment="LEFT",
            verticalAlignment="MIDDLE",
            textFormat=TextFormat(bold=True)
        )
        format_cell_range(ws_report, f"B{4}:E{4}", fmt_top_text)

        # 상단 안내문 텍스트 병합 (저희와 함께~, 2024년~, 정산 관련하여~, E-mail)
        ws_report.merge_cells(f"B{8}:E{8}")
        fmt_top_guide_1 = CellFormat(
            horizontalAlignment="LEFT",
            verticalAlignment="MIDDLE",
            textFormat=TextFormat(bold=False)
        )
        format_cell_range(ws_report, f"B{8}:E{8}", fmt_top_guide_1)
        ws_report.merge_cells(f"B{9}:E{9}")
        fmt_top_guide_2 = CellFormat(
            horizontalAlignment="LEFT",
            verticalAlignment="MIDDLE",
            textFormat=TextFormat(bold=False)
        )
        format_cell_range(ws_report, f"B{9}:E{9}", fmt_top_guide_2)
        ws_report.merge_cells(f"B{10}:E{10}")
        fmt_top_guide_3 = CellFormat(
            horizontalAlignment="LEFT",
            verticalAlignment="MIDDLE",
            textFormat=TextFormat(bold=False)
        )
        format_cell_range(ws_report, f"B{10}:E{10}", fmt_top_guide_3)
        ws_report.merge_cells(f"F{10}:G{10}")


        # 부가세 별도 오른쪽 정렬
        fmt_tax_right = CellFormat(horizontalAlignment="RIGHT")
        format_cell_range(ws_report, f"G{row_cursor_report_end-2}:G{row_cursor_report_end}", fmt_tax_right)

        # (테두리, 포매팅 등)
        bold_format = CellFormat(textFormat=TextFormat(bold=True))
        format_cell_range(ws_report, f"A13:B13", bold_format)
        format_cell_range(ws_report, f"A{row_cursor_album}:B{row_cursor_album}", bold_format)
        format_cell_range(ws_report, f"A{row_cursor_deduction}:B{row_cursor_deduction}", bold_format)
        format_cell_range(ws_report, f"A{row_cursor_rate}:B{row_cursor_rate}", bold_format)

        # apply_section_styles
        apply_section_styles(
            sheet_id=ws_report_id,
            spreadsheet_id=out_file_id,
            row_cursor_album=row_cursor_album,
            row_cursor_deduction=row_cursor_deduction,
            row_cursor_rate=row_cursor_rate,
            row_cursor_report_end=row_cursor_report_end,
            row_cursor_sum1=row_cursor_sum1,
            row_cursor_sum2=row_cursor_sum2,
            row_cursor_sum3=row_cursor_sum3,
            row_cursor_sum4=row_cursor_sum4
        )

    # 다음 달 탭 복제
    next_ym = get_next_month_str(ym)
    new_ws = duplicate_worksheet_with_new_name(song_cost_sh, ym, next_ym)
    
    new_data = new_ws.get_all_values()
    if not new_data:
        st.warning(f"'{ym}' 탭 복제 → '{next_ym}' 탭이 비어있습니다.")
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
                cur_remain_val = float(row_data[idxc]) if row_data[idxc] else 0.0
            except:
                cur_remain_val = 0.0
            row_data[idxp] = str(cur_remain_val)
            updated.append(row_data)
        if updated:
            new_ws.update("A2", updated, value_input_option="USER_ENTERED")

    return out_file_id


# ========== [5] Streamlit UI =============

def main():
    st.title("아티스트 음원 정산 보고서 자동 생성기")

    # 1) check_dict 준비
    check_dict = {
        "song_artists": [],
        "revenue_artists": [],
        "artist_compare_result": {},  # 여기에 누락 아티스트 등 정보
        # "mismatch_cost": {},   # cost vs 결과 불일치
        # "mismatch_revenue": {} # revenue vs 결과 불일치
    }

    ym = st.text_input("진행기간(YYYYMM)", "")
    report_date = st.text_input("보고서 발행 날짜 (YYYY-MM-DD)", "")

    if st.button("작업 시작하기"):
        if not ym or not re.match(r'^\d{6}$', ym):
            st.error("진행기간(YYYYMM) 6자리를 입력하세요.")
            return
        if not report_date:
            report_date = str(datetime.date.today())

        # 1) 보고서 생성
        out_file_id = generate_report(ym, report_date, check_dict)  # <--- 수정: check_dict 추가
        
        # 2) 보고서 생성 끝났으면 => check_dict 활용해 UI 표시

        if out_file_id:
            st.success(f"'{ym}' 진행기간 보고서 생성 완료! => {out_file_id}")
            st.info(f"생성된 파일 링크: https://docs.google.com/spreadsheets/d/{out_file_id}/edit")

            # 2) XLSX(zip) 다운로드 버튼
            #   report_sheets.zip 이름으로 다운로드
            #   download_all_tabs_as_zip(...) 호출
            zip_data = download_all_tabs_as_zip(out_file_id, credentials)
            st.download_button(
                label="XLSX(zip) 다운로드",
                data=zip_data,
                file_name=f"{ym}_report_sheets.zip",
                mime="application/zip"
            )
            
            # 3) 검증 결과 함수:
            def show_verification_result(check_dict):
                st.subheader("검증 결과")
                # 1) 원본 비교
                ar = check_dict.get("artist_compare_result", {})
                if ar:
                    st.write(f"- Song cost 아티스트 수: {ar['song_count']}")
                    st.write(f"- Revenue 아티스트 수: {ar['revenue_count']}")
                    st.write(f"- 공통 아티스트 수: {ar['common_count']}")
                    missing_in_song = ar["missing_in_song"]
                    missing_in_revenue = ar["missing_in_revenue"]
                    if missing_in_song:
                        st.warning(f"Song에 없고 Revenue에만 있는 아티스트: {missing_in_song}")
                    if missing_in_revenue:
                        st.warning(f"Revenue에 없고 Song에만 있는 아티스트: {missing_in_revenue}")
                else:
                    st.write("원본 비교 결과 없음")

            # 3) 검증 결과 표시:
            show_verification_result(check_dict)

if __name__ == "__main__":
    main()
