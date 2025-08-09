import os
import re
import time
import datetime
import pandas as pd
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError, ResumableUploadError

def clean_text(text):
    if not isinstance(text, str):
        text = str(text)
    text = re.sub(r'[\n\r]', ' ', text)
    text = re.sub(r'<br>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def get_yesterday_videos(channel_id, api_key, max_retries=3, initial_delay=2):
    youtube = None
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
    except Exception as e:
        print(f"YouTube API 빌드 중 오류 발생: {e}")
        return []

    uploads_playlist_id = None
    channel_name = 'N/A'
    for attempt in range(max_retries):
        try:
            channel_response = youtube.channels().list(
                part='contentDetails,snippet',
                id=channel_id
            ).execute()

            if not channel_response['items']:
                print(f"경고: 채널 ID '{channel_id}'에 대한 정보를 찾을 수 없습니다.")
                return []

            uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            channel_name = channel_response['items'][0]['snippet']['title']
            break # 성공 시 루프 탈출
        except HttpError as e:
            print(f"채널 플레이리스트 ID 가져오기 중 HTTP 오류 발생 (시도 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1 and e.resp.status in [403, 429, 500, 503]:
                time.sleep(initial_delay * (2 ** attempt))
            else:
                print(f"최대 재시도 횟수 초과 또는 치명적인 오류: {e}")
                return []
        except Exception as e:
            print(f"채널 플레이리스트 ID 가져오기 중 오류 발생 (시도 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(initial_delay * (2 ** attempt))
            else:
                return []
    
    if not uploads_playlist_id:
        print(f"오류: 채널 '{channel_id}'의 업로드 플레이리스트 ID를 가져오는 데 실패했습니다.")
        return []

    yesterday_utc = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)).date()

    yesterday_video_ids = []
    next_page_token = None
    page_count = 0
    max_pages = 5

    while True:
        playlist_items = []
        for attempt in range(max_retries):
            try:
                playlist_response = youtube.playlistItems().list(
                    part='snippet,contentDetails',
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                ).execute()
                playlist_items = playlist_response.get('items', [])
                break
            except HttpError as e:
                print(f"플레이리스트 항목 가져오기 중 HTTP 오류 발생 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1 and e.resp.status in [403, 429, 500, 503]:
                    time.sleep(initial_delay * (2 ** attempt))
                else:
                    print(f"최대 재시도 횟수 초과 또는 치명적인 오류: {e}")
                    return []
            except Exception as e:
                print(f"플레이리스트 항목 가져오기 중 오류 발생 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(initial_delay * (2 ** attempt))
                else:
                    return []

        if not playlist_items:
            break

        for item in playlist_items:
            publish_date_str = item['snippet']['publishedAt']
            publish_datetime_utc = datetime.datetime.fromisoformat(publish_date_str.replace('Z', '+00:00'))
            publish_date_only = publish_datetime_utc.date()
            
            if publish_date_only == yesterday_utc:
                video_id = item['contentDetails']['videoId']
                yesterday_video_ids.append(video_id)
            elif publish_date_only < yesterday_utc:
                next_page_token = None
                break 

        next_page_token = playlist_response.get('nextPageToken')
        page_count += 1
        if not next_page_token or page_count >= max_pages:
            break
    
    video_metadata = []
    parts_to_request = 'snippet,contentDetails,statistics,status,topicDetails,liveStreamingDetails'

    for i in range(0, len(yesterday_video_ids), 50):
        video_ids_chunk = yesterday_video_ids[i:i + 50]
        video_ids_str = ','.join(video_ids_chunk)

        for attempt in range(max_retries):
            try:
                video_info_response = youtube.videos().list(
                    part=parts_to_request,
                    id=video_ids_str
                ).execute()
                
                for video_item in video_info_response.get('items', []):
                    metadata = {
                        'channel_name': channel_name,
                        'video_id': video_item.get('id', 'N/A'),
                        'url': f'https://www.youtube.com/watch?v={video_item.get("id", "N/A")}',
                    }

                    # snippet 파트
                    snippet = video_item.get('snippet', {})
                    metadata['title'] = clean_text(snippet.get('title', 'N/A'))
                    metadata['description'] = clean_text(snippet.get('description', 'N/A'))
                    metadata['published_at'] = snippet.get('publishedAt', 'N/A')
                    metadata['channel_title'] = snippet.get('channelTitle', 'N/A')
                    metadata['tags'] = snippet.get('tags', [])
                    metadata['category_id'] = snippet.get('categoryId', 'N/A')
                    metadata['live_broadcast_content'] = snippet.get('liveBroadcastContent', 'N/A')

                    # contentDetails 파트
                    content_details = video_item.get('contentDetails', {})
                    metadata['duration'] = content_details.get('duration', 'N/A')

                    # statistics 파트
                    statistics = video_item.get('statistics', {})
                    metadata['view_count'] = statistics.get('viewCount', '0')
                    metadata['like_count'] = statistics.get('likeCount', '0')
                    metadata['favorite_count'] = statistics.get('favoriteCount', '0')
                    metadata['comment_count'] = statistics.get('commentCount', '0')

                    # status 파트
                    status = video_item.get('status', {})
                    metadata['upload_status'] = status.get('uploadStatus', 'N/A')
                    metadata['privacy_status'] = status.get('privacyStatus', 'N/A')

                    # topicDetails 파트
                    topic_details = video_item.get('topicDetails', {})
                    metadata['topic_categories'] = topic_details.get('topicCategories', [])

                    # liveStreamingDetails 파트
                    live_streaming_details = video_item.get('liveStreamingDetails', {})
                    metadata['actual_start_time'] = live_streaming_details.get('actualStartTime', 'N/A')
                    metadata['actual_end_time'] = live_streaming_details.get('actualEndTime', 'N/A')
                    metadata['concurrent_viewers'] = live_streaming_details.get('concurrentViewers', 'N/A')
                    metadata['archive_end_time'] = live_streaming_details.get('archiveEndTime', 'N/A')
                    
                    video_metadata.append(metadata)
                break
            except HttpError as e:
                print(f"동영상 정보 가져오기 중 HTTP 오류 발생 (ID chunk: {video_ids_chunk[0]}... , 시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1 and e.resp.status in [403, 429, 500, 503]:
                    time.sleep(initial_delay * (2 ** attempt))
                else:
                    print(f"최대 재시도 횟수 초과 또는 치명적인 오류로 동영상 정보 가져오기 실패. 해당 chunk의 필드는 'N/A'로 채워집니다.")

                    for vid_id in video_ids_chunk:
                        video_metadata.append({
                            'channel_name': channel_name, 'video_id': vid_id, 'url': f'https://www.youtube.com/watch?v={vid_id}',
                            'title': 'N/A', 'description': 'N/A', 'published_at': 'N/A', 'channel_title': 'N/A', 'tags': [],
                            'category_id': 'N/A', 'live_broadcast_content': 'N/A', 'duration': 'N/A', 'view_count': '0',
                            'like_count': '0', 'favorite_count': '0', 'comment_count': '0', 'upload_status': 'N/A',
                            'privacy_status': 'N/A', 'topic_categories': [], 'actual_start_time': 'N/A',
                            'actual_end_time': 'N/A', 'concurrent_viewers': 'N/A', 'archive_end_time': 'N/A'
                        })
                    break
            except Exception as e:
                print(f"동영상 정보 가져오기 중 오류 발생 (ID chunk: {video_ids_chunk[0]}... , 시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(initial_delay * (2 ** attempt))
                else:
                    print(f"알 수 없는 오류로 동영상 정보 가져오기 실패. 해당 chunk의 필드는 'N/A'로 채워집니다.")
                    for vid_id in video_ids_chunk:
                        video_metadata.append({
                            'channel_name': channel_name, 'video_id': vid_id, 'url': f'https://www.youtube.com/watch?v={vid_id}',
                            'title': 'N/A', 'description': 'N/A', 'published_at': 'N/A', 'channel_title': 'N/A', 'tags': [],
                            'category_id': 'N/A', 'live_broadcast_content': 'N/A', 'duration': 'N/A', 'view_count': '0',
                            'like_count': '0', 'favorite_count': '0', 'comment_count': '0', 'upload_status': 'N/A',
                            'privacy_status': 'N/A', 'topic_categories': [], 'actual_start_time': 'N/A',
                            'actual_end_time': 'N/A', 'concurrent_viewers': 'N/A', 'archive_end_time': 'N/A'
                        })
                    break
            
    return video_metadata

if __name__ == '__main__':
    channel_id_list_path = Path(__file__).resolve().parent / 'data' / 'youtube_news_channel_list.csv'

    try:
        channel_id_list_df = pd.read_csv(channel_id_list_path, encoding='utf-8-sig')
        channel_ids = channel_id_list_df['채널 ID'].tolist()
        channel_names = channel_id_list_df['채널명'].tolist()
        channels_to_process = dict(zip(channel_names, channel_ids))
    except FileNotFoundError:
        print(f"오류: CSV 파일을 찾을 수 없습니다: {channel_id_list_path}")
        raise
    except KeyError:
        print("오류: CSV 파일에 '채널 ID' 또는 '채널명' 열이 없습니다.")
        raise
    except Exception as e:
        print(f"CSV 파일 읽기 중 오류 발생: {e}")
        raise

    api_key = os.environ.get('YOUTUBE_API_KEY')

    if not api_key:
        print("오류: 환경 변수 'YOUTUBE_API_KEY'가 설정되지 않았습니다.")
        print("API 키를 GitHub Secrets에 추가하거나, 로컬 환경 변수로 설정해주세요.")
        exit(1)

    all_videos_data = []

    if channels_to_process:
        print(f"총 {len(channels_to_process)}개의 채널에서 동영상 데이터를 수집합니다.")
        for news_name, channel_id in channels_to_process.items():
            print(f"\n--- 채널 '{news_name}' (ID: {channel_id}) 어제 동영상 가져오기 시작 ---")
            videos_for_channel = get_yesterday_videos(channel_id, api_key)
            all_videos_data.extend(videos_for_channel)

            if videos_for_channel:
                print(f"채널 '{news_name}'에서 총 {len(videos_for_channel)}개의 어제(UTC 기준) 업로드된 동영상을 찾았습니다.")
                for video in videos_for_channel:
                    print(f"  제목: {video['title']} | URL: {video['url']}")
                print("-" * 20)
            else:
                print(f"채널 '{news_name}'에서 어제 업로드된 동영상을 찾지 못했거나 오류가 발생했습니다.")
        
        if all_videos_data:
            df = pd.DataFrame(all_videos_data)
            
            current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"youtube_videos_metadata_{current_time}.csv"
            
            try:
                df.to_csv(output_filename, index=False, encoding='utf-8-sig')
                print(f"\n모든 동영상 메타데이터를 '{output_filename}' 파일에 저장했습니다.")
                print(f"총 {len(all_videos_data)}개의 동영상 데이터가 저장되었습니다.")
            except Exception as e:
                print(f"\nCSV 파일 저장 중 오류 발생: {e}")
        else:
            print("\n수집된 동영상 데이터가 없어 CSV 파일을 생성하지 않습니다.")

    else:
        print("CSV 파일에서 유효한 채널 ID를 찾을 수 없습니다. 목록을 확인해주세요.")