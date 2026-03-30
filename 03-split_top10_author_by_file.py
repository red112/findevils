import pandas as pd
import os
import re

def split_top_10_authors_with_count(parquet_file_path):
    """
    상위 10개 계정의 데이터를 추출하여 
    파일명에 [댓글개수]_[author_id]를 포함해 개별 저장합니다.
    """
    if not os.path.exists(parquet_file_path):
        print(f"오류: 파일을 찾을 수 없습니다 -> {parquet_file_path}")
        return

    # 1. 데이터 로드
    df = pd.read_parquet(parquet_file_path)
    
    # 2. 상위 10개 author_id 및 개수 추출
    top_counts = df['author_id'].value_counts().head(10)
    top_10_ids = top_counts.index.tolist()
    
    # 3. 경로 및 기본 파일명 설정
    file_dir = os.path.dirname(parquet_file_path)
    base_name = os.path.splitext(os.path.basename(parquet_file_path))[0]
    
    print(f"--- 상위 10개 계정 개별 파일 생성 시작 (파일명에 개수 포함) ---")
    
    for i, auth_id in enumerate(top_10_ids, 1):
        # 해당 ID의 데이터 필터링 및 시간순 정렬
        author_df = df[df['author_id'] == auth_id].copy()
        author_df = author_df.sort_values(by='timestamp_usec')
        
        count = len(author_df)
        auth_name = author_df['author_name'].iloc[0]
        
        # 4. 파일명 안전하게 생성 (특수문자 제거)
        # Naming Rule: [기존파일명]_[댓글개수]_[author_id].parquet
        safe_id = re.sub(r'[\\/*?:"<>|]', '', auth_id) # 파일 시스템 금지 문자 제거
        new_file_name = f"{base_name}_{count}_{safe_id}.parquet"
        save_path = os.path.join(file_dir, new_file_name)
        
        # 5. 개별 파일 저장
        author_df.to_parquet(save_path, engine='pyarrow', compression='snappy', index=False)
        
        print(f"[{i:02d}] 생성 완료: {new_file_name} (이름: {auth_name})")

    print("-" * 30)
    print(f"분석용 파일 생성이 완료되었습니다. 위치: {file_dir}")

# --- 실행 설정 ---
# 앞서 저장한 필터링된 파일 경로를 입력하세요.
TARGET_FILTERED_PARQUET = './data/live_comments_table-01.morethan150.parquet' 

# 실행
split_top_10_authors_with_count(TARGET_FILTERED_PARQUET)