import datetime
import requests
import json
import pandas
import numpy
import io
import time
import re
from pytz import timezone
import streamlit as st
import pydeck as pdk

st.set_page_config(layout="wide")

FILE_BUFFER = io.BytesIO()
CLAIM_SECRETS = st.secrets["CLAIM_SECRETS"]
API_URL = st.secrets["API_URL"]
SECRETS_MAP = {"Melonn": 0,
               "Amoblando Pullman": 1,
               "Bogota test client": 2,
               "La Mansion": 3,
               "Sutex": 4,
               "Laika": 5,
               "Loto del Sur": 6,
               "Shopping Go": 7,
               "Guia Cereza": 8,
               "Distrihogar": 9,
               "Wild & Pacific": 10,
               "Studio F": 11,
               "Bukz": 12,
               "Tiendas Branchos": 13,
               "Exiagrícola": 14,
               "Distrisex": 15,
               "Vibes": 16,
               "Stop Jeans": 17,
               "Medivaric": 18,
               "Krika": 19,
               "Vitaliah": 20,
               "Pasarex": 21,
               "Crystal": 22,
               "Foodology": 23,
               "Pa Mascotas": 24,
               "Fiorenzi": 25,
               "Medipiel": 26,
               "Dermos": 27,
               "Teku": 28,
               "Undergold": 29,
               "Explora": 30,
               "PatPrimo": 31,               
               "Lunia": 32,
               "La Peau": 33,
               "Liza Herrera Calzado": 34,
               "Fenomena": 35,
               "Offcorss": 36,
               "Replays": 37,
               "Axspen": 38,
               "Alma de las cosas ": 39,
               "Filage": 40,
               "Vera y Estopa": 41}
CLIENTS_MAP = {0: "Melonn",
               1: "Amoblando Pullman",
               2: "Bogota test client",
               3: "La Mansion",
               4: "Sutex",
               5: "Laika",
               6: "Loto del Sur",
               7: "Shopping Go",
               8: "Guia Cereza",
               9: "Distrihogar",
               10: "Wild & Pacific",
               11: "Studio F",
               12: "Bukz",
               13: "Tiendas Branchos",
               14: "Exiagrícola",
               15: "Distrisex",
               16: "Vibes",
               17: "Stop Jeans",
               18: "Medivaric",
               19: "Krika",
               20: "Vitaliah",
               21: "Pasarex",
               22: "Crystal",
               23: "Foodology",
               24: "Pa Mascotas",
               25: "Fiorenzi",
               26: "Medipiel",
               27: "Dermos",
               28: "Teku",
               29: "Undergold",
               30: "Explora",
               31: "PatPrimo",
               32: "Lunia",
               33: "La Peau",
               34: "Liza Herrera Calzado",
               35: "Fenomena",
               36: "Offcorss",
               37: "Replays",
               38: "Axspen",
               39: "Alma de las cosas ",
               40: "Filage",
               41: "Vera y Estopa"}

statuses = {
    'delivered': {'type': '4. delivered', 'state': 'in progress'},
    'pickuped': {'type': '3. pickuped', 'state': 'in progress'},
    'returning': {'type': '3. pickuped', 'state': 'in progress'},
    'cancelled_by_taxi': {'type': 'X. cancelled', 'state': 'final'},
    'delivery_arrived': {'type': '3. pickuped', 'state': 'in progress'},
    'cancelled': {'type': 'X. cancelled', 'state': 'final'},
    'performer_lookup': {'type': '1. created', 'state': 'in progress'},
    'performer_found': {'type': '2. assigned', 'state': 'in progress'},
    'performer_draft': {'type': '1. created', 'state': 'in progress'},
    'returned': {'type': 'R. returned', 'state': 'in progress'},
    'returned_finish': {'type': 'R. returned', 'state': 'final'},
    'performer_not_found': {'type': 'X. cancelled', 'state': 'final'},
    'return_arrived': {'type': '3. pickuped', 'state': 'in progress'},
    'delivered_finish': {'type': '4. delivered', 'state': 'final'},
    'failed': {'type': 'X. cancelled', 'state': 'final'},
    'accepted': {'type': '1. created', 'state': 'in progress'},
    'new': {'type': '1. created', 'state': 'in progress'},
    'pickup_arrived': {'type': '2. assigned', 'state': 'in progress'},
    'estimating_failed': {'type': 'X. cancelled', 'state': 'final'},
    'cancelled_with_payment': {'type': 'X. cancelled', 'state': 'final'}
}    
    
def get_claims(secret, date_from, date_to, cursor=0):
    url = API_URL
  
    timezone_offset = "-05:00"
    payload = json.dumps({
        "created_from": f"{date_from}T00:00:00{timezone_offset}",
        "created_to": f"{date_to}T23:59:59{timezone_offset}",
        "limit": 1000,
        "cursor": cursor
    }) if cursor == 0 else json.dumps({"cursor": cursor})

    headers = {
        'Content-Type': 'application/json',
        'Accept-Language': 'en',
        'Authorization': f"Bearer {secret}"
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    claims = json.loads(response.text)
    cursor = None
    try:
        cursor = claims['cursor']
        print(f"CURSOR: {cursor}")
    except:
        print("LAST PAGE PROCESSED")
    return claims['claims'], cursor


def get_report(client_option="All clients", start_=None, end_=None) -> pandas.DataFrame:
    offset_back = 0
    client_timezone = "America/Bogota"
    today = datetime.datetime.now(timezone(client_timezone))
    date_from_offset = datetime.datetime.fromisoformat(start_).astimezone(
        timezone(client_timezone)) - datetime.timedelta(days=2)
    date_from = date_from_offset.strftime("%Y-%m-%d")
    date_to = end_

    today = today.strftime("%Y-%m-%d")
    report = []
    if client_option == "All clients":
      client_number = 0  
      for secret in CLAIM_SECRETS:
          #st.write(client_number)
          #st.write(secret)
          claims, cursor = get_claims(secret, date_from, date_to)
          while cursor:
              new_page_claims, cursor = get_claims(secret, date_from, date_to, cursor)
              claims = claims + new_page_claims
          for claim in claims:
              try:
                  claim_from_time = claim['same_day_data']['delivery_interval']['from']
              except:
                  continue
              cutoff_time = datetime.datetime.fromisoformat(claim_from_time).astimezone(timezone(client_timezone))
              cutoff_date = cutoff_time.strftime("%Y-%m-%d")
              if not start_:
                  if cutoff_date != today:
                      continue
              report_cutoff = cutoff_time.strftime("%Y-%m-%d %H:%M")
              report_client = CLIENTS_MAP[client_number]
              try:
                  report_client_id = claim['route_points'][1]['external_order_id'].replace("\t", " ")
              except:
                  report_client_id = "External order id is blanc"
              report_claim_id = claim['id']
              report_pickup_address = claim['route_points'][0]['address']['fullname']
              report_pod_point_id = str(claim['route_points'][1]['id'])
              report_receiver_address = claim['route_points'][1]['address']['fullname']
              report_receiver_phone = claim['route_points'][1]['contact']['phone']
              report_receiver_name = claim['route_points'][1]['contact']['name']
              try:
                  report_comment = claim['route_points'][1]['address']['comment']
              except:
                  report_comment = "No comment"
              report_status = claim['status']
              report_status_time = claim['updated_ts']
              report_store_name = claim['route_points'][0]['contact']['name']
              report_longitude = claim['route_points'][1]['address']['coordinates'][0]
              report_latitude = claim['route_points'][1]['address']['coordinates'][1]
              report_store_longitude = claim['route_points'][0]['address']['coordinates'][0]
              report_store_latitude = claim['route_points'][0]['address']['coordinates'][1]
              try: 
                  report_status_type = statuses[report_status]['type']
                  report_status_is_final = statuses[report_status]['state']
              except:
                  report_status_type = "?. other"
                  report_status_is_final = "unknown"
              try:
                  report_courier_name = claim['performer_info']['courier_name']
                  report_courier_park = claim['performer_info']['legal_name']
              except:
                  report_courier_name = "No courier yet"
                  report_courier_park = "No courier yet"
              try:
                  report_return_reason = str(claim['route_points'][1]['return_reasons'])
                  report_return_comment = str(claim['route_points'][1]['return_comment'])
              except:
                  report_return_reason = "No return reasons"
                  report_return_comment = "No return comments"
              try:
                  report_autocancel_reason = claim['autocancel_reason']
              except:
                  report_autocancel_reason = "No cancel reasons"
              try:
                  report_route_id = claim['route_id']
              except:
                  report_route_id = "No route"
              try:
                  report_price_of_goods = 0
                  for item in claim['items']:
                      report_price_of_goods += float(item['cost_value'])
              except:
                  report_price_of_goods = 0
              try:
                  report_goods = ""
                  for item in claim['items']:
                      report_goods = report_goods + str(item['title']) + " |"
              except:
                  report_goods = "Not specified"
              try:
                  report_weight_kg = 0.0
                  for item in claim['items']:
                      if re.findall(r"(\d*\.?\d+)\s*(kgs?)\b", str(item['title']), flags=re.IGNORECASE):
                          report_weight_kg = report_weight_kg + float(re.findall(r"(\d*\.?\d+)\s*(kgs?)\b", str(item['title']), flags=re.IGNORECASE)[0][0])
              except:
                  report_weight_kg = "Not found"
              try:
                  report_point_B_time = datetime.datetime.strptime(claim['route_points'][1]['visited_at']['actual'],"%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone(client_timezone))
                  report_point_B_time = report_point_B_time.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
              except:
                  report_point_B_time = "Point B was never visited"
              try:
                  report_point_A_time = datetime.datetime.strptime(claim['route_points'][0]['visited_at']['actual'],"%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone(client_timezone))
                  report_point_A_time = report_point_A_time.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
              except:
                  report_point_A_time = "Point A missing pick datetime"
              try:
                  report_general_comment = claim['comment']
              except:
                  report_general_comment = "No comment"
              row = [report_cutoff, report_client, report_client_id, report_claim_id, report_pod_point_id,
                    report_pickup_address, report_receiver_name, report_comment,
                    report_status, report_status_time, report_store_name, report_courier_name, report_courier_park,
                    report_return_reason, report_return_comment, report_autocancel_reason, report_route_id,
                    report_longitude, report_latitude, report_store_longitude, report_store_latitude, report_price_of_goods, report_goods, 
                    report_weight_kg, report_status_type, report_status_is_final, report_point_A_time, report_point_B_time, report_general_comment]
              report.append(row)
          client_number += 1
    else:
          secret = CLAIM_SECRETS[SECRETS_MAP[selected_client]]
          claims, cursor = get_claims(secret, date_from, date_to)
          while cursor:
                new_page_claims, cursor = get_claims(secret, date_from, date_to, cursor)
                claims = claims + new_page_claims
          for claim in claims:
              try:
                  claim_from_time = claim['same_day_data']['delivery_interval']['from']
              except:
                  continue
              cutoff_time = datetime.datetime.fromisoformat(claim_from_time).astimezone(timezone(client_timezone))
              cutoff_date = cutoff_time.strftime("%Y-%m-%d")
              if not start_:
                 if cutoff_date != today:
                    continue
              report_cutoff = cutoff_time.strftime("%Y-%m-%d %H:%M")
              report_client = selected_client
              try:
                  report_client_id = claim['route_points'][1]['external_order_id'].replace("\t", " ")
              except:
                  report_client_id = "unknown"
              report_claim_id = claim['id']
              report_pickup_address = claim['route_points'][0]['address']['fullname']
              report_pod_point_id = str(claim['route_points'][1]['id'])
              report_receiver_address = claim['route_points'][1]['address']['fullname']
              report_receiver_phone = claim['route_points'][1]['contact']['phone']
              report_receiver_name = claim['route_points'][1]['contact']['name']
              try:
                  report_comment = claim['route_points'][1]['address']['comment']
              except:
                  report_comment = "No comment"
              report_status = claim['status']
              report_status_time = claim['updated_ts']
              report_store_name = claim['route_points'][0]['contact']['name']
              report_longitude = claim['route_points'][1]['address']['coordinates'][0]
              report_latitude = claim['route_points'][1]['address']['coordinates'][1]
              report_store_longitude = claim['route_points'][0]['address']['coordinates'][0]
              report_store_latitude = claim['route_points'][0]['address']['coordinates'][1]
              try: 
                  report_status_type = statuses[report_status]['type']
                  report_status_is_final = statuses[report_status]['state']
              except:
                  report_status_type = "?. other"
                  report_status_is_final = "unknown"
              try:
                  report_courier_name = claim['performer_info']['courier_name']
                  report_courier_park = claim['performer_info']['legal_name']
              except:
                  report_courier_name = "No courier yet"
                  report_courier_park = "No courier yet"
              try:
                  report_return_reason = str(claim['route_points'][1]['return_reasons'])
                  report_return_comment = str(claim['route_points'][1]['return_comment'])
              except:
                  report_return_reason = "No return reasons"
                  report_return_comment = "No return comments"
              try:
                  report_autocancel_reason = claim['autocancel_reason']
              except:
                  report_autocancel_reason = "No cancel reasons"
              try:
                  report_route_id = claim['route_id']
              except:
                  report_route_id = "No route"
              try:
                  report_price_of_goods = 0
                  for item in claim['items']:
                      report_price_of_goods += float(item['cost_value'])
              except:
                  report_price_of_goods = 0
              try:
                  report_goods = ""
                  for item in claim['items']:
                      report_goods = report_goods + str(item['title']) + " |"
              except:
                  report_goods = "Not specified"
              try:
                  report_weight_kg = 0.0
                  for item in claim['items']:
                      if re.findall(r"(\d*\.?\d+)\s*(kgs?)\b", str(item['title']), flags=re.IGNORECASE):
                          report_weight_kg = report_weight_kg + float(re.findall(r"(\d*\.?\d+)\s*(kgs?)\b", str(item['title']), flags=re.IGNORECASE)[0][0])
              except:
                  report_weight_kg = "Not found"
              try:
                  report_point_B_time = datetime.datetime.strptime(claim['route_points'][1]['visited_at']['actual'],"%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone(client_timezone))
                  report_point_B_time = report_point_B_time.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
              except:
                  report_point_B_time = "Point B was never visited"
              try:
                  report_point_A_time = datetime.datetime.strptime(claim['route_points'][0]['visited_at']['actual'],"%Y-%m-%dT%H:%M:%S.%f%z").astimezone(timezone(client_timezone))
                  report_point_A_time = report_point_A_time.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
              except:
                  report_point_A_time = "Point A missing pick datetime"
              try:
                  report_general_comment = claim['comment']
              except:
                  report_general_comment = "No comment"
              row = [report_cutoff, report_client, report_client_id, report_claim_id, report_pod_point_id,
                    report_pickup_address, report_receiver_address, report_comment,
                    report_status, report_status_time, report_store_name, report_courier_name, report_courier_park,
                    report_return_reason, report_return_comment, report_autocancel_reason, report_route_id,
                    report_longitude, report_latitude, report_store_longitude, report_store_latitude, report_price_of_goods, report_goods, 
                    report_weight_kg, report_status_type, report_status_is_final, report_point_A_time, report_point_B_time, report_general_comment]
              report.append(row)
    result_frame = pandas.DataFrame(report,
                                    columns=["cutoff", "client", "client_id", "claim_id", "pod_point_id",
                                             "pickup_address", "receiver_address", "comment", "status", "status_time",
                                             "store_name", "courier_name", "courier_park",
                                             "return_reason", "return_comment", "cancel_comment",
                                             "route_id", "lon", "lat", "store_lon", "store_lat", "price_of_goods", "items",
                                             "extracted_weight", "type", "is_final", "point_a_time", "point_b_time", "general_comment"])
    return result_frame


st.markdown(f"# Routes report")

if st.sidebar.button("Refresh data", type="primary"):
    st.cache_data.clear()
st.sidebar.caption(f"Page reload doesn't refresh the data.\nInstead, use this button to get a fresh report")


startdate = st.sidebar.date_input("Start date")
enddate = st.sidebar.date_input("End date")
selected_client = st.sidebar.selectbox(
    "Select client:",
    ["Melonn", "Amoblando Pullman", "Bogota test client", "La Mansion", "Sutex",
     "Laika", "Loto del Sur", "Shopping Go", "Guia Cereza", "Distrihogar",
     "Wild & Pacific", "Studio F", "Bukz", "Tiendas Branchos", "Exiagrícola",
     "Distrisex", "Vibes", "Stop Jeans", "Medivaric", "Krika", "Vitaliah",
     "Pasarex", "Crystal", "Foodology","Pa Mascotas", "Fiorenzi", "Medipiel",
     "Dermos", "Teku", "Undergold", "Explora", "PatPrimo", "Lunia", "La Peau", "Liza Herrera Calzado", 
     "Fenomena", "Offcorss", "Replays", "Axspen", "Alma de las cosas ", "Filage", "Vera y Estopa", "All clients"]
)

@st.cache_data
def get_cached_report(client_option, start, end):
    date_start = start.strftime("%Y-%m-%d")
    date_end = end.strftime("%Y-%m-%d")
    report = get_report(client_option, start_=date_start, end_=date_end)
    df_rnt = report[~report['status'].isin(["cancelled", "performer_not_found", "failed"])]
    df_rnt = df_rnt.groupby(['courier_name', 'route_id', 'store_name'])['pickup_address'].nunique().reset_index()
    routes_not_taken = df_rnt[(df_rnt['courier_name'] == "No courier yet") & (df_rnt['route_id'] != "No route")]
    del df_rnt

    delivered_today = len(report[report['status'].isin(['delivered', 'delivered_finish'])])
    return report, routes_not_taken, delivered_today


df, routes_not_taken, delivered_today = get_cached_report(selected_client, startdate, enddate)

statuses = st.sidebar.multiselect(
    'Filter by status:',
    ['delivered',
     'pickuped',
     'returning',
     'cancelled_by_taxi',
     'delivery_arrived',
     'cancelled',
     'performer_lookup',
     'performer_found',
     'performer_draft',
     'returned',
     'returned_finish',
     'performer_not_found',
     'return_arrived',
     'delivered_finish',
     'failed',
     'accepted',
     'new',
     'pickup_arrived'])

stores = st.sidebar.multiselect(
    "Filter by stores:",
    df["store_name"].unique()
)

couriers = st.sidebar.multiselect(
    "Filter by courier:",
    df["courier_name"].unique()
)

without_cancelled = st.sidebar.checkbox("Without cancels")

if without_cancelled:
    df = df[~df["status"].isin(["cancelled", "performer_not_found", "failed", "estimating_failed", "cancelled_by_taxi", "cancelled_with_payment"])]    
    
col1, col3 = st.columns(2)
col1.metric("Not pickuped routes :minibus:", str(len(routes_not_taken)))
col3.metric(f"Delivered :package:", delivered_today)

if (not statuses or statuses == []) and (not stores or stores == []):
    filtered_frame = df
elif statuses and not stores:
    filtered_frame = df[df['status'].isin(statuses)]
elif stores and not statuses:
    filtered_frame = df[df['store_name'].isin(stores)]
else:
    filtered_frame = df[(df['store_name'].isin(stores)) & (df['store_name'].isin(statuses))]

if couriers:
    filtered_frame = df[df['courier_name'].isin(couriers)]

st.dataframe(filtered_frame)

client_timezone = "America/Bogota"
TODAY = datetime.datetime.now(timezone(client_timezone))

if st.checkbox("enable download"):
    with pandas.ExcelWriter(FILE_BUFFER, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='routes_report')
        writer.close()
        st.download_button(label="Download report as xlsx",data=FILE_BUFFER,file_name=f"route_report_{TODAY}.xlsx",mime="application/vnd.ms-excel")

    
with st.expander(":round_pushpin: Orders on a map"):
    st.caption(
        f'Hover order to see details. Stores are the big points on a map. :green[Green] orders are delivered, and :red[red] – are the in delivery state. :orange[Orange] are returned or returning. Gray are cancelled.')
    chart_data_delivered = filtered_frame[filtered_frame["status"].isin(['delivered', 'delivered_finish'])]
    chart_data_in_delivery = filtered_frame[~filtered_frame["status"].isin(
        ['delivered', 'delivered_finish', 'cancelled', 'cancelled_by_taxi', 'returning', 'returned_finish',
         'return_arrived'])]
    chart_data_returns = filtered_frame[
        filtered_frame["status"].isin(['returning', 'returned_finish', 'return_arrived'])]
    chart_data_cancels = filtered_frame[filtered_frame["status"].isin(['cancelled', 'cancelled_by_taxi'])]
    view_state_lat = filtered_frame['lat'].iloc[0]
    view_state_lon = filtered_frame['lon'].iloc[0]
    filtered_frame['cutoff'] = filtered_frame['cutoff'].str.split(' ').str[1]
    stores_on_a_map = filtered_frame.groupby(['store_name', 'store_lon', 'store_lat'])['cutoff'].agg(
        lambda x: ', '.join(x.unique())).reset_index(drop=False)
    stores_on_a_map.columns = ['store_name', 'store_lon', 'store_lat', 'cutoff']
    st.pydeck_chart(pdk.Deck(
        map_style=None,
        height=1200,
        initial_view_state=pdk.ViewState(
            latitude=view_state_lat,
            longitude=view_state_lon,
            zoom=10,
            pitch=0,
        ),
        tooltip={"text": "{store_name} : {cutoff}\n{courier_name} : {status}\n{client_id} : {claim_id}"},
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_delivered,
                get_position='[lon, lat]',
                get_color='[11, 102, 35, 160]',
                get_radius=200,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_in_delivery,
                get_position='[lon, lat]',
                get_color='[200, 30, 0, 160]',
                get_radius=200,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_cancels,
                get_position='[lon, lat]',
                get_color='[215, 210, 203, 200]',
                get_radius=200,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=chart_data_returns,
                get_position='[lon, lat]',
                get_color='[237, 139, 0, 160]',
                get_radius=200,
                pickable=True
            ),
            pdk.Layer(
                'ScatterplotLayer',
                data=filtered_frame,
                get_position='[store_lon, store_lat]',
                get_color='[0, 128, 255, 160]',
                get_radius=250,
                pickable=True
            ),
            pdk.Layer(
                'TextLayer',
                data=stores_on_a_map,
                get_position='[store_lon, store_lat]',
                get_text='store_name',
                get_color='[0, 128, 255]',
                get_size=14,
                get_pixel_offset='[0, 20]',
                pickable=False
            ),
            pdk.Layer(
                'TextLayer',
                data=stores_on_a_map,
                get_position='[store_lon, store_lat]',
                get_text='cutoff',
                get_color='[0, 128, 255]',
                get_size=14,
                get_pixel_offset='[0, 40]',
                pickable=False
            )
        ],
    ))
