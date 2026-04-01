import streamlit as st
import folium
from streamlit_folium import st_folium
import requests
import time
import hashlib
import hmac
import base64
import urllib.parse
import pandas as pd
import xmltodict
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt
from concurrent.futures import ThreadPoolExecutor

# --- [1. 보안 인증 정보 설정] ---
# 깃허브 경고 방지를 위해 st.secrets에서 값을 가져옵니다.
# 배포 후 Streamlit Cloud 설정(Settings > Secrets)에 키들을 입력해야 작동합니다.
IAM_ACCESS_KEY = st.secrets["IAM_ACCESS_KEY"]
IAM_SECRET_KEY = st.secrets["IAM_SECRET_KEY"]
CLIENT_ID = st.secrets["CLIENT_ID"]
CLIENT_SECRET = st.secrets["CLIENT_SECRET"]
MOLIT_KEY = st.secrets["MOLIT_KEY"]

# --- [2. 세션 상태 초기화] ---
if 'coords_cache' not in st.session_state: st.session_state.coords_cache = {}
if 'lat' not in st.session_state: st.session_state.lat, st.session_state.lon = 37.4742, 127.1053
if 'address' not in st.session_state: st.session_state.address = "서울특별시 성동구 고산자로14길 26"
if 'lawd_cd' not in st.session_state: st.session_state.lawd_cd = "11680"
if 'df_filtered' not in st.session_state: st.session_state.df_filtered = None
if 'collected_data' not in st.session_state: st.session_state.collected_data = []


# --- [3. 유틸리티 함수] ---
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dLat, dLon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dLat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dLon / 2) ** 2
    return R * 2 * asin(sqrt(a))


def make_signature(uri, timestamp):
    message = "GET " + uri + "\n" + timestamp + "\n" + IAM_ACCESS_KEY
    signing_key = base64.b64encode(
        hmac.new(bytes(IAM_SECRET_KEY, 'UTF-8'), bytes(message, 'UTF-8'), digestmod=hashlib.sha256).digest())
    return signing_key.decode('UTF-8')


def get_coords_and_code_pure(address):
    """NCP API를 이용한 좌표 및 법정동 코드 획득 (쓰레드 안전 버전)"""
    timestamp = str(int(time.time() * 1000))
    geo_uri = f"/map-geocode/v2/geocode?query={urllib.parse.quote(address)}"
    headers = {"x-ncp-iam-access-key": IAM_ACCESS_KEY, "x-ncp-apigw-timestamp": timestamp,
               "x-ncp-apigw-signature-v2": make_signature(geo_uri, timestamp),
               "X-NCP-APIGW-API-KEY-ID": CLIENT_ID, "X-NCP-APIGW-API-KEY": CLIENT_SECRET}
    try:
        res = requests.get(f"https://maps.apigw.ntruss.com{geo_uri}", headers=headers, timeout=5)
        if res.status_code == 200:
            addr_data = res.json().get('addresses')
            if not addr_data: return None
            lon, lat = addr_data[0]['x'], addr_data[0]['y']

            rev_uri = f"/map-reversegeocode/v2/gc?coords={lon},{lat}&orders=legalcode&output=json"
            rev_headers = {"x-ncp-iam-access-key": IAM_ACCESS_KEY, "x-ncp-apigw-timestamp": timestamp,
                           "x-ncp-apigw-signature-v2": make_signature(rev_uri, timestamp),
                           "X-NCP-APIGW-API-KEY-ID": CLIENT_ID, "X-NCP-APIGW-API-KEY": CLIENT_SECRET}
            rev_res = requests.get(f"https://maps.apigw.ntruss.com{rev_uri}", headers=rev_headers, timeout=5)
            full_code = "1168066200"
            if rev_res.status_code == 200:
                results = rev_res.json().get('results', [])
                if results: full_code = results[0].get('code', {}).get('id', '1168066200')
            return (float(lat), float(lon), full_code, addr_data[0].get('roadAddress', address))
    except:
        pass
    return None


def fetch_and_filter_radius(lawd_cd, category, center_lat, center_lon, radius_km, months_count, min_p, max_p,
                            target_depo):
    all_data = []
    months = [(datetime.now().replace(day=1) - timedelta(days=30 * i)).strftime("%Y%m") for i in range(months_count)]
    mapping = {"오피스텔": "RTMSDataSvcOffiRent", "아파트": "RTMSDataSvcAptRent", "단독/다가구": "RTMSDataSvcSHRent"}
    endpoint = mapping.get(category, "RTMSDataSvcOffiRent")
    base_url = f"http://apis.data.go.kr/1613000/{endpoint}/get{endpoint}"

    prog = st.progress(0)
    for i, month in enumerate(months):
        params = {'serviceKey': MOLIT_KEY, 'LAWD_CD': lawd_cd, 'DEAL_YMD': month, 'returnType': 'xml',
                  'numOfRows': '1000'}
        try:
            res = requests.get(base_url, params=params, timeout=10)
            d = xmltodict.parse(res.content)
            items = d.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            if items: all_data.extend(items if isinstance(items, list) else [items])
        except:
            pass
        prog.progress((i + 1) / len(months))

    if not all_data: return pd.DataFrame()
    df_raw = pd.DataFrame(all_data)

    area_col = 'excluUseAr' if 'excluUseAr' in df_raw.columns else 'totalFloorAr'
    if area_col in df_raw.columns:
        df_raw[area_col] = df_raw[area_col].astype(float)
        df_raw['전용면적(평)'] = (df_raw[area_col] / 3.3058).round(1)
        df_raw = df_raw[(df_raw['전용면적(평)'] >= min_p) & (df_raw['전용면적(평)'] <= max_p)]

    if df_raw.empty: return pd.DataFrame()

    name_col = 'offiNm' if 'offiNm' in df_raw.columns else ('aptNm' if 'aptNm' in df_raw.columns else 'umdNm')
    city_prefix = " ".join(st.session_state.address.split()[:2])
    unique_bldgs = df_raw[[name_col, 'umdNm', 'jibun']].drop_duplicates()

    addrs_to_fetch = [f"{city_prefix} {r['umdNm']} {r['jibun']}" for _, r in unique_bldgs.iterrows() if
                      f"{city_prefix} {r['umdNm']} {r['jibun']}" not in st.session_state.coords_cache]

    if addrs_to_fetch:
        with ThreadPoolExecutor(max_workers=10) as exe:
            results = list(exe.map(get_coords_and_code_pure, addrs_to_fetch))
            for addr, res in zip(addrs_to_fetch, results):
                if res: st.session_state.coords_cache[addr] = res

    filtered = []
    for _, r in df_raw.iterrows():
        b_addr = f"{city_prefix} {r['umdNm']} {r['jibun']}"
        cache_data = st.session_state.coords_cache.get(b_addr)
        if cache_data:
            dist = haversine(center_lat, center_lon, cache_data[0], cache_data[1])
            if dist <= radius_km:
                try:
                    depo_orig = int(str(r.get('deposit', r.get('보증금액', 0))).replace(',', ''))
                    rent_orig = int(str(r.get('monthlyRent', r.get('월세금액', 0))).replace(',', ''))
                    std_rent = rent_orig + ((depo_orig - target_depo) * 0.05 / 12)
                    build_y = int(r.get('buildYear', 0))

                    filtered.append({
                        'lat': cache_data[0], 'lon': cache_data[1], 'dist': dist, '단지명': r[name_col],
                        '층': r.get('floor', '-'), '보증금': depo_orig * 10, '임대료': rent_orig * 10,
                        '환산 임대료': round(std_rent * 10, 0),
                        '평당 임대료': round((std_rent * 10) / r['전용면적(평)'], 0) if r['전용면적(평)'] > 0 else 0,
                        '건축년도': build_y, '경과년수': 2026 - build_y if build_y > 0 else 0,
                        '법정동코드10': cache_data[2], '지번': r.get('jibun'), 'umdNm': r['umdNm'],
                        '거래월': f"{r['dealYear']}.{r['dealMonth']}", '전용면적(평)': r['전용면적(평)'],
                        'sggCd': r.get('sggCd') or r.get('sigunguCode') or lawd_cd
                    })
                except:
                    pass
    prog.empty()
    return pd.DataFrame(filtered)


# --- [4. UI 구성] ---
st.set_page_config(layout="wide", page_title="SLP 시장조사 헬퍼")
st.title("개발솔루션팀 시장조사 헬퍼")

with st.sidebar:
    st.header("조건 설정")
    category = st.selectbox("물건 유형", ["오피스텔", "아파트", "단독/다가구"])

    st.divider()
    st.subheader("보증금 설정 (천원)")
    depo_slider = st.slider("환산 기준 보증금", 0, 100000, 5000, step=1000)
    target_depo_1000 = st.number_input("보증금 직접 입력", 0, 100000, depo_slider)
    target_depo_orig = target_depo_1000 / 10

    st.divider()
    st.subheader("면적 및 기간 설정")
    p_slider = st.slider("전용면적 범위 (평)", 0, 100, (8, 12))
    p_min = st.number_input("최소 평수", 0, 100, p_slider[0])
    p_max = st.number_input("최대 평수", 0, 100, p_slider[1])

    months_slider = st.slider("수집 개월 수", 1, 36, 12)
    months_input = st.number_input("개월 수 직접 입력", 1, 36, months_slider)

    st.divider()
    st.subheader("반경 설정 (m)")
    radius_slider = st.slider("분석 반경", 100, 5000, 300, step=100)
    radius_input = st.number_input("반경 직접 입력", 100, 5000, radius_slider)

    # 데이터 로드 후 준공년도 필터 활성화
    if st.session_state.df_filtered is not None and not st.session_state.df_filtered.empty:
        st.divider()
        st.subheader("결과 내 연식 필터")
        min_y = int(st.session_state.df_filtered['건축년도'].min())
        max_y = int(st.session_state.df_filtered['건축년도'].max())
        year_filter = st.slider("준공년도 범위", min_y, max_y, (min_y, max_y))

    st.divider()
    if st.button("데이터 분석 시작", use_container_width=True):
        st.session_state.df_filtered = fetch_and_filter_radius(
            st.session_state.lawd_cd, category, st.session_state.lat, st.session_state.lon,
            radius_input / 1000, months_input, p_min, p_max, target_depo_orig
        )

# 메인 주소 검색창
search_q = st.text_input("분석 중심 주소", value=st.session_state.address)
if search_q != st.session_state.address:
    res = get_coords_and_code_pure(search_q)
    if res:
        st.session_state.update(
            {'lat': res[0], 'lon': res[1], 'lawd_cd': res[2][:5], 'address': res[3], 'df_filtered': None})
        st.rerun()

# 필터링 적용된 데이터 구성
df_view = st.session_state.df_filtered
if df_view is not None and not df_view.empty and 'year_filter' in locals():
    df_view = df_view[(df_view['건축년도'] >= year_filter[0]) & (df_view['건축년도'] <= year_filter[1])]

col1, col2 = st.columns([2, 1])

with col1:
    m = folium.Map(location=[st.session_state.lat, st.session_state.lon], zoom_start=16)
    folium.TileLayer(tiles="https://xdworld.vworld.kr/2d/LandTalk/service/{z}/{x}/{y}.png", attr='Vworld', overlay=True,
                     opacity=0.6).add_to(m)
    folium.Marker([st.session_state.lat, st.session_state.lon],
                  icon=folium.Icon(color='lightblue', icon='info-sign')).add_to(m)
    folium.Circle([st.session_state.lat, st.session_state.lon], radius=radius_input, color='skyblue', fill=True,
                  opacity=0.3).add_to(m)

    if df_view is not None and not df_view.empty:
        collected_names = [d['단지명'] for d in st.session_state.collected_data]
        bldg_summary = df_view.groupby(['단지명', 'lat', 'lon'])['평당 임대료'].mean().reset_index()
        for _, r in bldg_summary.iterrows():
            # [수집 전: 회색, 수집 후: 파란색]
            m_color = 'blue' if r['단지명'] in collected_names else 'lightgray'
            folium.Marker([r['lat'], r['lon']],
                          tooltip=f"{r['단지명']} (평당 {r['평당 임대료']:,.0f}천원)",
                          icon=folium.Icon(color=m_color, icon='home')).add_to(m)
    map_data = st_folium(m, width="100%", height=600,
                         key=f"map_{st.session_state.lat}_{st.session_state.lon}_{'filter' if 'year_filter' in locals() else ''}")

with col2:
    st.subheader("단지 상세 정보 및 수집")
    if df_view is not None and not df_view.empty:
        st.write("반경 내 전체 데이터 요약")
        st.caption(f"평균 환산 임대료: {df_view['환산 임대료'].mean():,.0f}천원 / 평균 평당 임대료: {df_view['평당 임대료'].mean():,.0f}천원")
        st.divider()

    clicked = map_data.get("last_object_clicked")
    if clicked and df_view is not None:
        c_lat, c_lon = clicked['lat'], clicked['lng']
        detail_df = df_view[(abs(df_view['lat'] - c_lat) < 0.0001) & (abs(df_view['lon'] - c_lon) < 0.0001)]
        if not detail_df.empty:
            row = detail_df.iloc[0]
            h_col1, h_col2 = st.columns([2, 1])
            with h_col1:
                st.markdown(f"### {row['단지명']}")
            with h_col2:
                search_query = f"{row['umdNm']} {row['단지명']}"
                naver_url = f"https://m.land.naver.com/search/result/{urllib.parse.quote(search_query)}"
                st.link_button("상세정보", naver_url)

            st.write(f"준공: {row['건축년도']}년 ({row['경과년수']}년차)")
            is_duplex = st.checkbox("복층", key=f"dup_{row['단지명']}")
            room_type = st.radio("룸 타입", ["1R", "1.5R", "2R", "3R", "4R"], horizontal=True, key=f"room_{row['단지명']}")

            if st.button("수집 데이터 추가", use_container_width=True):
                area = detail_df['전용면적(평)'].mean() * (1.35 if is_duplex else 1.0)
                rent = detail_df['환산 임대료'].mean()
                st.session_state.collected_data.append({
                    '단지명': row['단지명'], '타입': room_type, '복층여부': 'O' if is_duplex else 'X',
                    '전용면적(평)': round(area, 1), '환산 임대료': round(rent, 0), '평당 임대료': round(rent / area, 0),
                    '연식': row['경과년수']
                })
                st.rerun()
            st.divider()
            disp = detail_df[['거래월', '전용면적(평)', '층', '보증금', '임대료', '환산 임대료', '평당 임대료']].copy()
            for col in ['보증금', '임대료', '환산 임대료', '평당 임대료']: disp[col] = disp[col].map('{:,.0f}천원'.format)
            st.dataframe(disp.sort_values('거래월', ascending=False), use_container_width=True)

# --- [5. 수집된 시장조사 데이터 요약 리스트] ---
st.divider()
st.subheader("수집된 시장조사 데이터 요약")
if st.session_state.collected_data:
    c_df = pd.DataFrame(st.session_state.collected_data)
    s1, s2 = st.columns(2)
    s1.metric("수집 평균 환산 임대료", f"{c_df['환산 임대료'].mean():,.0f} 천원")
    s2.metric("수집 평균 평당 임대료", f"{c_df['평당 임대료'].mean():,.0f} 천원")
    st.dataframe(c_df, use_container_width=True)
    if st.button("수집 리스트 전체 삭제"): st.session_state.collected_data = []; st.rerun()