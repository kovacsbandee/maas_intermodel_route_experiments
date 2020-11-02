'''
Stacionárius hálózat:
A közlekedési hálózat modelljét az egyéni közlekedési módokhoz a 'transport network graph' adja meg,
mely egy súlyozott és irányított gráf, ahol a csúcsok a kereszteződések, az élek pedig az utak,
az élek hosszát egy idődimenziójú (sec) súlyfüggvény adja meg, amit az él hosszából és
vagy a maxspeed változóból, vagy az 5 km/h-ás gyaloglási sebességből számítok ki.


Időfüggő hálózat:
Jelülje S a megállók halmazát, melyek fizikailag jelen vannak a hálózatban, egy megállót több járat is kiszolgálhat,
t.f.h egy megállót n darab járat szolgál ki, ekkor hozzá rendelünk a megállóhoz n darab virtuális csúcsot,
melyek a konstans utazási idő modellezéséhez szükségesek. A virtuális csúcsok közti átszállási idők konstansok.
A hálózat csúcsai: megállók csúcsai,
                   virtuális járat csúcsok (mindegyik megállóhoz, S db járat és mindegyik Si járathoz
                   tartozik n db járat csúcs)

A hálózat élei: a virtuális járat csúcsok és a megálló csúcsai közötti élek, a
                a megálló csúcsok és a virtuális járat csúcsok közötti élek, illetve
                az azonos járathoz tartozó megállók közti élek.

A graph_tool-ban van útkereső algoritmus, mivel a GTD gráf-ban általános keresőkkel is lehet útvonalat keresni,
ezért az megfelelő.
'''
import numpy as np
import pandas as pd
pd.set_option('display.max_columns', 20)
import urbanaccess as ua
from urbanaccess.network import ua_network
import time
from geopy import distance

file_path ='/work/maas/'

nodes, edges = ua.osm.load.ua_network_from_bbox(bbox = (18.6554,46.9197,20.2768,48.0891), network_type = 'walk')
nodes.to_csv(f'{file_path}osm_graph/nodes.csv', index = False)
edges.to_csv(f'{file_path}osm_graph/edges.csv', index = False)

nodes, edges = ua.osm.load.ua_network_from_bbox(bbox = (18.6554,46.9197,20.2768,48.0891), network_type = 'drive')
nodes.to_csv(f'{file_path}osm_graph/nodes_drive.csv', index = False)
edges.to_csv(f'{file_path}osm_graph/edges_drive.csv', index = False)

def convert_calendar_date_to_calendar(file_path = f'{file_path}budapest_gtfs/'):
    '''
    Elkészíti a hiányzó calendar.txt file-t a hiányzó BKK GTFS feed-ből.
    :param file_path:
    :return:
    '''
    calendar_dates = pd.read_csv(filepath_or_buffer = f'{file_path}calendar_dates.txt',
                                 parse_dates = ['date'],
                                 index_col = False)

    # exception_type = 1, ha a szervíz típust hozzáadták az adott dátumhoz, 2 ha elvették
    if calendar_dates['exception_type'] != 1:
        print('Nem konzisztens az exception_type mező az eddigiekkel, mert van benne 1-től eltérő érték!!!')
    else:
        calendar_dates['day'] = pd.Series(calendar_dates['date']).dt.day_name().str.lower()

        calendar = pd.DataFrame(columns=['service_id',
                                         'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
                                         'start_date', 'end_date'])
        calendar['service_id'] = calendar_dates['service_id'].unique()
        for service_id in calendar_dates['service_id']:
            calendar_dates_serv_id = pd.DataFrame(calendar_dates[calendar_dates['service_id'] == service_id])
            calendar['start_date'][calendar['service_id'] == service_id] = calendar_dates_serv_id['date'].min()
            calendar['end_date'][calendar['service_id'] == service_id] = calendar_dates_serv_id['date'].max()
            days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            check_dict = {day : pd.date_range(start=calendar[calendar['service_id'] == service_id]['start_date'].to_string(index=False),
                                             end=calendar[calendar['service_id'] == service_id]['end_date'].to_string(index=False),
                                             freq = f'W-{days[0][0:3].upper()}').strftime('%Y-%m-%d').to_list() \
                               for day in days}
            # csak akkor teszem bele az 1 flag-et a calendar adott napjának oszlopába,
            # ha a vizsgált időszak minden vonatkozó napján megvolt az adott szolgáltatás
            for day in days:
                if all(calendar_dates_serv_id[calendar_dates_serv_id['day']==day]['date'].isin(check_dict[day])):
                    calendar.set_value(calendar[calendar['service_id'] == service_id].index, day, 1)
        calendar = calendar.fillna(0)
        calendar.to_csv(f'{file_path}calendar.txt')

#convert_calendar_date_to_calendar()

def create_TD_graph(gtfs_file_path = '/work/maas/budapest_gtfs/',
                    #departure_time = 0,
                    transfer_time = 180,
                    v_avg_pt = 30):
    '''
    # a trips-ben 166-al több  route_id van, mint a routes-ban (járat), ezért azokat a route_id-kat fogom használni
    # a stop_times-ban 490-el kevesebb stop_id van mint a stops-ban, itt viszont a stop_times-ban lévő stop_id-kat fogom használni,
    # mert nem fogom tudni becsatolni a trip_route_df-et a stops-ban lévő összes stop_id-ba a stop_times-on keresztül
    :param file_path:
    :param departure_time: az indulási idő másodpercben, egyelőre a fv. hívás idejét használom
    :param transfer_time: a konstans átszállási idő, ez egyelőre 180 sec, de ezt később pontosítani szükséges,
                          az adott megállóban lévő járat pár közti átszálláshoz szükséges idővel.
    :param v_avg_pt: a közösségi közlekedés átlagos sebessége két megálló között km/h-ban, ezt adott viszonylathoz lehetne igazítani, amennyiben ismert annak az átlag sebessége
    :return:
    '''
    #departure_time = time.perf_counter()
    stops = pd.read_csv(f'{gtfs_file_path}stops.txt')
    stop_times = pd.read_csv(f'{gtfs_file_path}stop_times.txt')
    trips = pd.read_csv(f'{gtfs_file_path}trips.txt')
    TD_nodes = pd.DataFrame(columns=['TD_id', 'TD_lat', 'TD_lon'])
    TD_nodes = TD_nodes.append(stops[['stop_id', 'stop_lat', 'stop_lon']].\
                               rename(columns = {'stop_id': 'TD_id', 'stop_lat': 'TD_lat', 'stop_lon': 'TD_lon'}),
                               ignore_index=False)
    # a connector graph és  a TD graph közti összekötés lehet rossz, mert
    # a stops-ban ugye kevesebb stop_id van mint a stop_times-ban!
    route_nodes_base_df = pd.merge(
                                pd.merge(trips,
                                         stop_times,
                                         on='trip_id',
                                         how='left'),
                                stops,
                                on='stop_id',
                                how='left')
    # a route_node_id neve a route_id és a stop_id összevonásából keletkezik
    route_nodes_base_df['route_node_id'] = route_nodes_base_df['route_id'].astype(str)+route_nodes_base_df['stop_id'].astype(str)
    route_nodes = route_nodes_base_df[['route_node_id', 'stop_lat', 'stop_lon']].drop_duplicates()
    TD_nodes = TD_nodes.append(route_nodes.\
                               rename(columns = {'route_node_id': 'TD_id', 'stop_lat': 'TD_lat', 'stop_lon': 'TD_lon'}),
                               ignore_index=False)
    TD_edges = pd.DataFrame(columns=['stop_id', 'route_node_id', 'weight'])
    TD_edges_A = stops.merge(route_nodes, on=['stop_lat', 'stop_lon'], how='inner')[['stop_id', 'route_node_id']]
    TD_edges_A['weight'] = transfer_time
    TD_edges_B = TD_edges_A[['route_node_id', 'stop_id', 'weight']]
    TD_edges_C_tuple_list = []
    C_edges_base_df = pd.merge(stop_times, trips, on='trip_id', how='left')

    route_s = C_edges_base_df['route_id'].unique()
    for route in route_s:
        trip_s = C_edges_base_df[C_edges_base_df['route_id'] == route]['trip_id'].unique()
        for trip in trip_s:
            route_stops = C_edges_base_df[(C_edges_base_df['route_id'] == route) &\
                                          (C_edges_base_df['trip_id'] == trip)][['stop_id', 'shape_dist_traveled']]
            for i in range(route_stops.index[0], route_stops.index[-2]):
                TD_edges_C_tuple_list.append((route_stops['stop_id'][i],
                                              route_stops['stop_id'][i+1],
                                              (route_stops['shape_dist_traveled'][i+1] - route_stops['shape_dist_traveled'][i])\
                                              / (v_avg_pt * 1000/3600)))
    TD_edges_C = pd.DataFrame(TD_edges_C_tuple_list, columns=['stop_id_i', 'stop_id_i1', 'weight'])


    TD_edges = TD_edges.append([TD_edges_A,TD_edges_B, TD_edges_C])
    TD_nodes.to_csv('TD_nodes.csv', index=False)
    TD_edges.to_csv('TD_edges.csv', index=False)

def create_transport_graph(file_path = file_path,
                           node_file = 'nodes.csv',
                           edges_file = 'edges.csv',
                           drive = False,
                           walk = True):
    '''
    OSM graph betöltése az élek súlyának elkészítése (az él bejárásához szükséges idő másodpercben).
    Az irányítottság már benne van, mert a mindkét irányba járható utak mindkét irányítottsággal benne vannak,
    különböző sorban.
    :param file_path:
    :param node_file:
    :param edges_file:
    :param drive:
    :param walk:
    :return:
    '''
    road_nodes = pd.read_csv(f'{file_path}{node_file}')
    road_edges = pd.read_csv(f'{file_path}{edges_file}')
    road_edges['maxspeed'] = road_edges['maxspeed'].fillna(road_edges['maxspeed'].mean())
    if drive:
        road_edges['weight'] = road_edges['distance'] / (road_edges['maxspeed'] * 1000/3600)
        road_edges['net_type'] = 'drive'
        ua_network.osm_nodes = road_nodes
        ua_network.osm_edges = road_edges[['from', 'to', 'weight', 'net_type']]
        ua_network.osm_nodes.to_csv('TR_graph_nodes_drive.csv', index=False)
        ua_network.osm_edges.to_csv('TR_graph_edges_drive.csv', index=False)
    if walk:
        road_edges['weight'] = road_edges['distance'] / (5 * 1000/3600)
        road_edges['net_type'] = 'walk'
        ua_network.osm_nodes = road_nodes
        ua_network.osm_edges = road_edges[['from', 'to', 'weight', 'net_type']]
        ua_network.osm_nodes.to_csv('TR_graph_nodes_walk.csv', index=False)
        ua_network.osm_edges.to_csv('TR_graph_edges_walk.csv', index=False)



def create_connector_graph(file_path = '/work/maas/',
                           gtfs_path = '/work/maas/budapest_gtfs/',
                           filt_dist = 0.0025):
    '''
    össze kell kötni a stop_id-kat és a road_node-okat a koordinátájuk alapján,
    oly módon, hogy a legközelebbi road_node-ot kell bekötni az adott stop_node-hoz,
    első közelítésben leválogatom az elég közel lévő road_node-okat,
    majd azok távolságát kiszámolván kiírom a legkisebb távolságú id-ját a stop_id mellé
    '''
    stop_nodes = pd.read_csv(f'{gtfs_path}stops.txt')
    road_nodes = pd.read_csv(f'{file_path}nodes.csv').\
        rename(columns = {'x': 'road_lon', 'y': 'road_lat', 'id': 'road_node_id'}).\
        astype({'road_node_id': 'object'})
    connector_edges = []
    for stop in stop_nodes['stop_id'].unique():
    #stop = stop_nodes['stop_id'].unique()[0]
        stop_point = stop_nodes[stop_nodes['stop_id'] == stop][['stop_lon', 'stop_lat']]
        road_points = \
        road_nodes[(stop_point['stop_lat'].item() - filt_dist < road_nodes['road_lat']) & (road_nodes['road_lat'] < stop_point['stop_lat'].item() + filt_dist) & \
                   (stop_point['stop_lon'].item() - filt_dist < road_nodes['road_lon']) & (road_nodes['road_lon'] < stop_point['stop_lon'].item() + filt_dist)]
        road_points['distance'] = [distance.geodesic(road_points[['road_lon', 'road_lat']].iloc[i], stop_point.values).m for i in range(len(road_points))]
        connector_edges.append((stop,
                                road_points[road_points['distance'] == road_points['distance'].min()]['road_node_id'].item(),
                                road_points[road_points['distance'] == road_points['distance'].min()]['distance'].item() / \
                                (5 * 1000/3600)))
    connector_edges_df = pd.DataFrame(connector_edges, columns=['stop_id', 'road_node_id', 'weight'])
    connector_nodes_df = pd.DataFrame(
        np.concatenate(
            (connector_edges_df['stop_id'].unique(),connector_edges_df['road_node_id'].unique())), columns=['conncetor_node_ids'])
    connector_edges_df.to_csv(f'{file_path}connector_edges.csv', index=False)
    connector_nodes_df.to_csv(f'{file_path}connector_nodes.csv', index=False)


# return connector_graph_nodes, connector_graph_edges
# adott stop_node-hoz legközelebbi road_node pár, illetve a köztük lévő távolság gyalog történő megtételéhez szüksége idő

import networkx as nx


def concatenate_TD_graph(gtfs_file_path = '/work/maas/budapest_gtfs/',
                        departure_time = 0,
                        transfer_time = 180,
                        v_avg_pt = 30):
    '''
    # a trips-ben 166-al több  route_id van, mint a routes-ban (járat), ezért azokat a route_id-kat fogom használni
    # a stop_times-ban 490-el kevesebb stop_id van mint a stops-ban, itt viszont a stop_times-ban lévő stop_id-kat fogom használni,
    # mert nem fogom tudni becsatolni a trip_route_df-et a stops-ban lévő összes stop_id-ba a stop_times-on keresztül
    :param file_path:
    :param departure_time: az indulási idő másodpercben, egyelőre a fv. hívás idejét használom
    :param transfer_time: a konstans átszállási idő, ez egyelőre 180 sec, de ezt később pontosítani szükséges,
                          az adott megállóban lévő járat pár közti átszálláshoz szükséges idővel.
    :param v_avg_pt: a közösségi közlekedés átlagos sebessége két megálló között km/h-ban, ezt adott viszonylathoz lehetne igazítani, amennyiben ismert annak az átlag sebessége
    :return:
    '''
    departure_time = time.perf_counter()
    stops = pd.read_csv(f'{gtfs_file_path}stops.txt')
    stop_times = pd.read_csv(f'{gtfs_file_path}stop_times.txt')
    trips = pd.read_csv(f'{gtfs_file_path}trips.txt')
    TD_nodes = pd.DataFrame(columns=['TD_id', 'TD_lat', 'TD_lon'])
    # először a stop node-okat rakom bele
    TD_nodes = TD_nodes.append(stops[['stop_id', 'stop_lat', 'stop_lon']].\
                               rename(columns = {'stop_id': 'TD_id', 'stop_lat': 'TD_lat', 'stop_lon': 'TD_lon'}),
                               ignore_index=False)
    # a connector graph és  a TD graph közti összekötés lehet rossz, mert
    # a stops-ban ugye kevesebb stop_id van mint a stop_times-ban!
    route_nodes_base_df = pd.merge(
                                pd.merge(trips,
                                         stop_times,
                                         on='trip_id',
                                         how='left'),
                                stops,
                                on='stop_id',
                                how='left')
    # a route_node_id neve a route_id és a stop_id összevonásából keletkezik
    route_nodes_base_df['route_node_id'] = route_nodes_base_df['route_id'].astype(str)+route_nodes_base_df['stop_id'].astype(str)
    route_nodes = route_nodes_base_df[['route_node_id', 'stop_lat', 'stop_lon']].drop_duplicates()
    TD_nodes = TD_nodes.append(route_nodes.\
                               rename(columns = {'route_node_id': 'TD_id', 'stop_lat': 'TD_lat', 'stop_lon': 'TD_lon'}),
                               ignore_index=False)
    TD_edges = pd.DataFrame(columns=['stop_id', 'route_node_id', 'weight'])
    TD_edges_A = stops.merge(route_nodes, on=['stop_lat', 'stop_lon'], how='inner')[['stop_id', 'route_node_id']]
    TD_edges_A['weight'] = transfer_time
    TD_edges_B = TD_edges_A[['route_node_id', 'stop_id', 'weight']]
    TD_edges_C = pd.read_csv('/work/maas/GTD_network_raw_data/TD_edges_C.csv').drop_duplicates()


    TD_edges = pd.DataFrame(columns=['TD_id_v', 'TD_id_w', 'weight'])
    TD_edges = TD_edges.append([TD_edges_A.rename(columns = {'stop_id': 'TD_id_w', 'route_node_id': 'TD_id_v'}),
                                TD_edges_B.rename(columns = {'route_node_id': 'TD_id_v', 'stop_id': 'TD_id_w'}),
                                TD_edges_C.rename(columns = {'stop_id_i': 'TD_id_v', 'stop_id_i1': 'TD_id_w'})])

    TD_nodes.to_csv('/work/maas/GTD_network_raw_data/TD_nodes.csv', index=False)
    TD_edges.to_csv('/work/maas/GTD_network_raw_data/TD_edges.csv', index=False)



# def create GTD_graph():
TR_edges_w = pd.read_csv('/work/maas/GTD_network_raw_data/TR_graph_edges_walk.csv')
TR_edges_d = pd.read_csv('/work/maas/GTD_network_raw_data/TR_graph_edges_drive.csv')
TR_nodes_w = pd.read_csv('/work/maas/GTD_network_raw_data/TR_graph_nodes_walk.csv')
TR_nodes_d = pd.read_csv('/work/maas/GTD_network_raw_data/TR_graph_nodes_drive.csv')

TR_nodes = pd.concat([TR_nodes_w, TR_nodes_d]).drop_duplicates()
TR_edges = pd.concat(([TR_edges_w, TR_edges_d]))

TD_nodes = pd.read_csv('/work/maas/GTD_network_raw_data/TD_nodes.csv').\
    rename(columns= {'TD_id': 'id', 'TD_lat': 'y', 'TD_lon':'x'})
TD_edges = pd.read_csv('/work/maas/GTD_network_raw_data/TD_edges.csv').\
    drop(['index'], axis=1).\
    rename(columns = {'TD_id_v': 'from', 'TD_id_w': 'to'})

connector_nodes = pd.read_csv('/work/maas/GTD_network_raw_data/connector_edges.csv').\
    rename(columns = {'stop_id':'from', 'road_node_id':'to'})

GTD_nodes = pd.concat([TD_nodes, TR_nodes])
GTD_edges = pd.concat([TD_edges, TR_edges, connector_nodes])

GTD_graph = nx.Graph()
GTD_graph.add_nodes_from(GTD_nodes['id'])
GTD_graph.add_weighted_edges_from([tuple(x) for x in GTD_edges[['from', 'to', 'weight']].to_numpy()])


random_id1 = GTD_nodes.sample()['id'].item()
random_id2 = GTD_nodes.sample()['id'].item()
start_time = time.time()
node_list = nx.astar_path(GTD_graph, random_id1,random_id2)
print(time.time()-start_time)



gtfs_feed_dfs = ua.gtfsfeed_to_df('/work/maas/budapest_gtfs')

transit_net = ua.gtfs.network.create_transit_net(gtfs_feed_dfs,
                                                 day = 'monday',
                                                 timerange = ['06:00:00', '18:00:00'])

osm_edges = pd.read_csv('/work/maas/osm_graph/edges_drive.csv')
osm_nodes = pd.read_csv('/work/maas/osm_graph/nodes_drive.csv')