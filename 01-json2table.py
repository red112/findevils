import json
import pandas as pd
from datetime import datetime
import os

def process_and_save_binary(file_path):
    """
    JSON 파일을 읽어 Pandas DataFrame으로 변환하고, 
    동일 경로에 .parquet 바이너리 형식으로 저장합니다.
    """
    parsed_data = []
    
    if not os.path.exists(file_path):
        print(f"오류: 파일을 찾을 수 없습니다 -> {file_path}")
        return None

    # 1. 데이터 추출 및 정규화
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line: continue
                
            try:
                data = json.loads(line)
                action_root = data.get("replayChatItemAction", {})
                actions = action_root.get("actions", [])
                video_offset = action_root.get("videoOffsetTimeMsec", None)
                
                for action in actions:
                    item = action.get("addChatItemAction", {}).get("item", {})
                    chat_renderer = item.get("liveChatTextMessageRenderer")
                    
                    if chat_renderer:
                        message_runs = chat_renderer.get("message", {}).get("runs", [])
                        full_message = "".join([run.get("text", "") for run in message_runs])
                        
                        row = {
                            "author_name": str(chat_renderer.get("authorName", {}).get("simpleText", "")),
                            "author_id": str(chat_renderer.get("authorExternalChannelId", "")),
                            "message": full_message,
                            "timestamp_usec": int(chat_renderer.get("timestampUsec", 0)),
                            "video_offset_ms": int(video_offset) if video_offset else 0,
                            "client_id": str(action.get("addChatItemAction", {}).get("clientId", "")),
                            "display_time": str(chat_renderer.get("timestampText", {}).get("simpleText", ""))
                        }
                        
                        if row["timestamp_usec"] > 0:
                            # 바이너리 저장 시 datetime 객체로 변환하여 저장 (타입 보존)
                            row["datetime"] = datetime.fromtimestamp(row["timestamp_usec"] / 1_000_000)
                        
                        parsed_data.append(row)
                        
            except json.JSONDecodeError:
                continue

    df = pd.DataFrame(parsed_data)
    if df.empty:
        print("추출된 데이터가 없습니다.")
        return None
    
    # 시간순 정렬
    df = df.sort_values(by='timestamp_usec').reset_index(drop=True)

    # 2. 경로 설정 및 바이너리(Parquet) 저장
    file_dir = os.path.dirname(file_path)
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    # 확장자를 .parquet으로 설정
    save_path = os.path.join(file_dir, f"{file_name}_table.parquet")

    # 엔진은 'pyarrow'를 권장하며, 스냅피(snappy) 압축을 기본으로 사용합니다.
    try:
        df.to_parquet(save_path, engine='pyarrow', compression='snappy', index=False)
        print(f"--- 변환 성공 ---")
        print(f"저장 위치: {save_path}")
        print(f"데이터 건수: {len(df)}건")
        print(f"파일 용량: {os.path.getsize(save_path) / 1024:.2f} KB")
    except ImportError:
        print("알림: pyarrow 라이브러리가 필요합니다. 'pip install pyarrow'를 실행해 주세요.")

    return df

# --- 실행 ---
TARGET_FILE = './data/live_comments.json' 
df_result = process_and_save_binary(TARGET_FILE)