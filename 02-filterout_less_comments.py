import pandas as pd
import os

def filter_by_comment_count(file_path, min_count=5):
    """
    Parquet 파일을 읽어 지정된 개수(min_count) 미만의 댓글을 가진 ID를 삭제하고 저장합니다.
    """
    if not os.path.exists(file_path):
        print(f"오류: 파일을 찾을 수 없습니다 -> {file_path}")
        return None

    # 1. 바이너리(Parquet) 파일 읽기
    df = pd.read_parquet(file_path)
    print(f"원본 데이터 로드 완료: {len(df)}건")

    # 2. 각 ID(author_id)별 댓글 개수 계산
    id_counts = df['author_id'].value_counts()

    # 3. 기준치(min_count) 이상인 ID만 추출
    keep_ids = id_counts[id_counts >= min_count].index
    filtered_df = df[df['author_id'].isin(keep_ids)].copy()

    # 4. 파일 저장 설정 (.parquet -> -01.morethanN.parquet)
    file_dir = os.path.dirname(file_path)
    # 확장자를 제외한 파일명 추출
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    # 'table' 등 기존 접미사가 있다면 제거하거나 유지 (여기서는 사용자 요청 형식 적용)
    new_file_name = f"{base_name}-01.morethan{min_count}.parquet"
    save_path = os.path.join(file_dir, new_file_name)

    # 바이너리 형태로 저장
    filtered_df.to_parquet(save_path, engine='pyarrow', compression='snappy', index=False)
    
    print("-" * 30)
    print(f"필터링 완료 (기준: {min_count}개 이상 작성자만 유지)")
    print(f"남은 댓글 수: {len(filtered_df)}건 (삭제된 댓글: {len(df) - len(filtered_df)}건)")
    print(f"저장 경로: {save_path}")
    print("-" * 30)

    # 5. 상위 10개 ID 및 개수 출력
    top_10 = filtered_df['author_id'].value_counts().head(10)
    
    # ID와 이름을 매칭해서 보여주기 위해 그룹화 (가독성용)
    top_10_info = filtered_df[filtered_df['author_id'].isin(top_10.index)].groupby('author_id').agg({
        'author_name': 'first',
        'message': 'count'
    }).sort_values(by='message', ascending=False)

    print("\n[상위 10개 활성 계정 분석 결과]")
    print(top_10_info.rename(columns={'message': '댓글 개수'}))

    return filtered_df

# --- 실행 설정 ---
# 이전에 저장된 .parquet 파일 경로를 입력하세요.
TARGET_PARQUET = './data/live_comments_table.parquet' 

# 최소 댓글 개수 기준 설정 
MIN_THRESHOLD = 150 

# 실행
filtered_data = filter_by_comment_count(TARGET_PARQUET, min_count=MIN_THRESHOLD)