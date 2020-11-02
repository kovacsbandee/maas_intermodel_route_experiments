import pandas as pd
import time
import multiprocessing
from joblib import Parallel, delayed
from tqdm import tqdm

num_cores = multiprocessing.cpu_count()

#def create_TD_graph(
gtfs_file_path = '/work/maas/budapest_gtfs/'
departure_time = 0
transfer_time = 180
v_avg_pt = 30#):
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


#C_edges_base_df = pd.merge(trips[['route_id','trip_id', 'shape_id']], stop_times[['trip_id', 'stop_id', 'shape_dist_traveled']], on='trip_id')
C_edges_base_df = pd.merge(stop_times, trips, on='trip_id', how='left')

splitting_val = 20

route_s = C_edges_base_df['route_id'].unique()

splitted_route_list = []
for i in range(int(len(route_s)/splitting_val)):
    splitted_route_list.append(route_s[i:i+int(len(route_s)/splitting_val)])

def create_TD_edges_C(route_s):
    TD_edges_C_tuple_list = []
    for route in route_s:
        trip_s = C_edges_base_df[C_edges_base_df['route_id'] == route]['trip_id'].unique()
        for trip in trip_s:
            route_stops = C_edges_base_df[(C_edges_base_df['route_id'] == route) & \
                                          (C_edges_base_df['trip_id'] == trip)][['stop_id', 'shape_dist_traveled']]
            for i in range(route_stops.index[0], route_stops.index[-2]):
                TD_edges_C_tuple_list.append((route_stops['stop_id'][i],
                                              route_stops['stop_id'][i + 1],
                                              (route_stops['shape_dist_traveled'][i + 1] -
                                               route_stops['shape_dist_traveled'][i]) \
                                              / (v_avg_pt * 1000 / 3600)))
    TD_edges_C = pd.DataFrame(TD_edges_C_tuple_list, columns=['stop_id_i', 'stop_id_i1', 'weight'])
    return TD_edges_C

num_cores = multiprocessing.cpu_count()
inputs = tqdm(splitted_route_list)

TD_edges_C_df_list = Parallel(n_jobs=num_cores)(delayed(create_TD_edges_C)(route_s) for route_s in inputs)

TD_edges_C = pd.concat(TD_edges_C_df_list, columns=['stop_id_i', 'stop_id_i1', 'weight'])
TD_edges_C.to_csv('TD_edges_C.csv', index=False)


TD_edges = TD_edges.append([TD_edges_A, TD_edges_B, TD_edges_C])
TD_nodes.to_csv('TD_nodes.csv', index=False)
TD_edges.to_csv('TD_edges.csv', index=False)
