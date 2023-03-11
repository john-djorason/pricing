"""
A module for pharmacies' pricing
"""

import numpy as np
import pandas as pd
import ext_connections as ext_con
import os
import requests
import zipfile
import datetime


class PricingSettings:
    """
    A class for pricing settings.

    There are 5 different pre-calculated data:
    1. A tuple of price segments // (100, 300, 500, 1000, 2000, 3000)
    2. A tuple of distance segments // (300, 500, 1000, 2000, 5000)
    3. A default distance unit in meters // 100
    4. A price per distance unit in UAH // 2
    5. A starting deviation // 0.005

    *First realization involves manual settings input
    """

    _settings = {}

    def __init__(self, file_name='settings.ini'):
        curr_dir = os.getcwd()
        if curr_dir[-1] != '\\':
            curr_dir += '\\'

        file_name = curr_dir + file_name
        with open(file_name, 'r') as file:
            for line in file.readlines():
                set_arr = line.split('=')
                key = set_arr[0].strip()
                value_str = line.replace(key + '=', '').strip()

                if value_str:
                    try:
                        if value_str[-1] == ']':
                            # Parameter array of numbers
                            value_str = value_str[1:-1]
                            arr_str = value_str.split(',')
                            value = [float(number_str.strip()) for number_str in arr_str]
                        else:
                            # Parameter number
                            value = float(value_str)
                    except ValueError as e:
                        # Parameter string
                        value = value_str
                else:
                    # No parameter at all
                    value = ''

                self._settings[key] = value

    def __str__(self):
        return str(self._settings)

    def __bool__(self):
        is_configured = True
        for element in self._settings.items():
            if not element[1]:
                is_configured = False
                break

        return is_configured

    def get_setting(self, name):
        return self._settings.get(name, '')


class PricingSchedule:
    _tasks = pd.DataFrame([])
    _default_settings = None

    def __init__(self):
        self._default_settings = PricingSettings()
        self._set_schedule()

    @property
    def default_settings(self):
        return self._default_settings

    def run(self):
        if not self.default_settings:
            return

        print('Starting... ', datetime.datetime.now())

        success_ids = []
        ind = 0
        count = len(self._tasks)
        for row in self._tasks.iterrows():
            ind += 1
            task = row[1]

            pharm_id = task['ID_Branch']
            ent_str = task['Code']
            sn_str = task['SerialNumber']
            if not ent_str or not sn_str:
                continue

            str_info = 'Pharmacy %s/%s (%s): ' % (ind, count, sn_str)
            print(str_info, 'Calculating')

            enterprise_code = int(ent_str)
            serial_number = int(sn_str)

            new_pricing = GoodsPricing(enterprise_code, serial_number, pharm_id, self.default_settings)
            if new_pricing.execute():
                success_ids.append(pharm_id)
                print(str_info, 'Success')
            else:
                print(str_info, 'Failure')

        self._del_schedule(success_ids)

        print('Finished... ', datetime.datetime.now())

    def _set_schedule(self):
        """Gets DataFrame of current tasks"""

        settings = self.default_settings
        url_tasks = settings.get_setting('tasks_api')
        authorization = settings.get_setting('auth')

        headers = {
            "Authorization": ' '.join(["Basic", authorization]),
            "Content-type": "application/json"
        }
        method = 'GET'
        content_type = 'json'

        connection = ext_con.TabletkiAPI()
        result_table = connection.execute(url_tasks, method, headers, content_type)
        connection.disconnect()

        if result_table is None or result_table.empty:
            return

        result_table.sort_values(by='DateTime')
        self._tasks = result_table

    def _del_schedule(self, pharm_ids):
        """Deletes task from pool"""

        if not pharm_ids:
            return False

        settings = self.default_settings
        url_tasks_delete = settings.get_setting('tasks_delete_api')
        authorization = settings.get_setting('auth')

        headers = {
            "Authorization": ' '.join(["Basic", authorization]),
            "Content-type": "application/json",
            "Accept": "application/json"
        }

        items = []
        for pharm_id in pharm_ids:
            item = {
                'ID_Branch': pharm_id
            }
            items.append(item)
        json_data = {'Items': items}

        try:
            respond = requests.post(url=url_tasks_delete, headers=headers, json=json_data)
            if respond.status_code != 200:
                return False

            json_response = respond.json()
            if json_response['Status'] == 'Error':
                print('Error:', json_response['Description'])
                return False
        except ConnectionResetError as e:
            print('Error:', e)
            return False
        except requests.exceptions.ConnectionError as e:
            print('Error:', e)
            return False

        return True


class GoodsPricing:
    """
    A class for pharmacies' goods pricing.

    The class is designed for realtime calculating and previous calculation
    of pharmacy's prices. After input data is received
    it's possible to create calculation math. models.

    This is first math. model realization:
    - Manual prices/distances segmentation with value per N meters coefficients usage in algorithm
    """

    _enterprise_code = 0
    _serial_number = 0
    _id_pharmacy = ''
    _settings = None
    _ratio_table = None
    _pharmacy_table = None
    _distance_table = None
    _pharmacy_prices = None
    _competitors_prices = None
    _new_prices = None
    _min_date = None

    def __init__(self, ent_code, pharmacy_code, pharmacy_id, settings=None):
        self._enterprise_code = ent_code
        self._serial_number = pharmacy_code
        self._id_pharmacy = pharmacy_id.upper()
        self._settings = settings
        if self.settings is None or not self.settings:
            self._settings = PricingSettings()

    @property
    def enterprise_code(self):
        return self._enterprise_code

    @property
    def serial_number(self):
        return self._serial_number

    @property
    def id_pharmacy(self):
        return self._id_pharmacy

    @property
    def settings(self):
        return self._settings

    @property
    def ratio_table(self):
        return self._ratio_table

    @property
    def pharmacy_table(self):
        return self._pharmacy_table

    @property
    def pharmacy_prices(self):
        return self._pharmacy_prices

    @property
    def competitors_prices(self):
        return self._competitors_prices

    @property
    def new_prices(self):
        return self._new_prices

    @property
    def distance_table(self):
        return self._distance_table

    def execute(self, new_settings=None):
        if not self.recalculate(new_settings=new_settings):
            return False
        if not self.make_pricing():
            return False
        if not self.save_prices():
            return False

        return True

    def recalculate(self, new_settings=None):
        if new_settings:
            self._settings = new_settings

        if not self.settings:
            return False

        if not self._calculate_ratio_table():
            return False
        if not self._calculate_pharmacy_table():
            return False
        if not self._calculate_distance_table():
            return False
        if not self._set_current_pharmacy_prices():
            return False

        return True

    def make_pricing(self):
        if not self._calculate_pharmacies_prices():
            return False
        if not self._set_new_pharmacy_prices():
            return False

        return True

    def save_prices(self):
        prices = self.new_prices
        if prices.empty:
            return False

        save_path = self.settings.get_setting('save_path')
        if not save_path:
            save_path = os.getcwd()

        save_path += '\\' + str(self.enterprise_code)
        if not os.path.exists(save_path):
            try:
                os.mkdir(save_path)
            except OSError:
                print('Creation of the directory %s failed' % save_path)

        file_name_no_ext = 'rest_' + str(self.serial_number) + '_' + self._min_date.strftime('%Y%m%d%H%M%S')
        file_name = file_name_no_ext + '.xml'
        full_path = save_path + '\\' + file_name
        ext_con.TabletkiParser.df_to_xml(prices, full_path, 'Offer')

        archive_name = save_path + '\\' + file_name_no_ext + '.zip'
        with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED) as z_file:
            z_file.write(full_path)

        if os.path.exists(full_path):
            os.remove(full_path)

        return True

    @staticmethod
    def distances_in_meters(lats, lngs):
        # approximate radius of Earth in meters
        radius = 6373000.0

        lats = np.radians(lats)
        lngs = np.radians(lngs)

        lat_matrix_1 = np.array([lats for _ in range(len(lats))])
        lat_matrix_2 = lat_matrix_1.T

        lng_matrix_1 = np.array([lngs for _ in range(len(lngs))])
        lng_matrix_2 = lng_matrix_1.T

        dlat = lat_matrix_2 - lat_matrix_1
        dlon = lng_matrix_2 - lng_matrix_1

        # Haversine formula
        a = np.sin(dlat / 2) ** 2 + np.cos(lat_matrix_1) * np.cos(lat_matrix_2) * np.sin(dlon / 2) ** 2
        c = 2 * np.arctan2(a ** 0.5, (-1 * a + 1) ** 0.5)

        distance = radius * c
        distance = distance.astype(int)

        return distance

    @staticmethod
    def get_distance(lat1, lng1, lat2, lng2):
        lats = [lat1, lat2]
        lngs = [lng1, lng2]
        distances = GoodsPricing.distances_in_meters(lats, lngs)

        return distances[0][1]

    def _as_code(self, pharm_id):
        df = self.pharmacy_table
        row = df[(df['ID_Branch'] == pharm_id)]
        return '' if row.empty else row['SerialNumber'].iloc[0]

    def _get_ratio(self, price_range, distance_range):
        return 0 if not self.settings else self.ratio_table[distance_range][price_range]

    def _get_ratio_matrix(self):
        settings = self.settings

        if not settings:
            return None

        prices = settings.get_setting('prices')
        distances = settings.get_setting('distances')
        def_unit = settings.get_setting('default_unit')
        unit_price = settings.get_setting('default_unit_price')
        deviation = settings.get_setting('deviation')

        ratio_matrix = []

        pre_price_value = 0
        for price_value in prices:
            price_sum = price_value + pre_price_value

            pre_dist_value = 0
            new_list = []
            for distance_value in distances:
                if pre_dist_value == 0:
                    ratio = 1 - deviation
                else:
                    dist_sum = distance_value + pre_dist_value
                    ratio = 1 + (dist_sum * unit_price) / (price_sum * def_unit)

                new_list.append(ratio)
                pre_dist_value = distance_value

            ratio_matrix.append(new_list)
            pre_price_value = price_value

        return ratio_matrix

    def _calculate_ratio_table(self):
        if not self.settings:
            self._ratio_table = None
            return False

        matrix = self._get_ratio_matrix()
        prices = self.settings.get_setting('prices')
        distances = self.settings.get_setting('distances')

        price_index = list(prices)
        distance_columns = list(distances)
        arr = np.array(matrix)

        df = pd.DataFrame(arr, index=price_index, columns=distance_columns)
        self._ratio_table = df

        return True

    def _calculate_distance_table(self):
        pharm_df = self.pharmacy_table
        if pharm_df is None:
            self._distance_table = None
            return False

        branches = pharm_df.ID_Branch.tolist()
        lats = pharm_df.Lat.tolist()
        lngs = pharm_df.Lng.tolist()

        distances = GoodsPricing.distances_in_meters(lats, lngs)

        table = pd.DataFrame(distances, index=branches, columns=branches)
        self._distance_table = table

        return True

    def _calculate_pharmacy_table(self):
        settings = self.settings
        url_pharmacies = settings.get_setting('branches_api')

        connection = ext_con.TabletkiAPI()
        df = connection.execute(url_pharmacies)
        connection.disconnect()

        if 'Lat' and 'Lng' and 'ID_Branch' not in df.columns:
            self._pharmacy_table = None
            return False

        # Divider for Latitude and Longitude from ClickHouse
        divider = 100000000.

        df.Lat = df.Lat / divider
        df.Lng = df.Lng / divider

        df['Code'] = df['Code'].astype(int)
        df['SerialNumber'] = df['SerialNumber'].astype(int)

        self._pharmacy_table = df

        return True

    def _calculate_pharmacies_prices(self):
        if not self.settings:
            return False

        distance_tuple = self.settings.get_setting('distances')
        last_dist = distance_tuple[-1]

        nearest_competitors = tuple(self._get_nearest_competitors(0, last_dist))
        all_prices = self._get_pharmacies_prices(pharmacies=nearest_competitors)
        self._competitors_prices = all_prices

        return True

    def _set_current_pharmacy_prices(self):
        settings = self.settings
        if not settings:
            self._pharmacy_prices = None
            self._min_date = None
            return False

        url_prices = settings.get_setting('prices_api')
        url_prices_by_pharm = url_prices + '&code=' + str(self.enterprise_code) + '&idBranch=' + self.id_pharmacy
        authorization = settings.get_setting('auth')

        headers = {
            "Authorization": ' '.join(["Basic", authorization]),
            "Content-type": "application/json"
        }
        method = 'GET'
        content_type = 'json'

        connection = ext_con.TabletkiAPI()

        result_table = connection.execute(url_prices_by_pharm, method, headers, content_type)
        result_table['Quantity'] = result_table['Quantity'].str.replace(',', '.')
        result_table['Quantity'] = result_table['Quantity'].astype(float)
        result_table['Price'] = result_table['Price'].str.replace(',', '.')
        result_table['Price'] = result_table['Price'].astype(float)
        result_table['PriceReserve'] = result_table['PriceReserve'].str.replace(',', '.')
        result_table['PriceReserve'] = result_table['PriceReserve'].astype(float)

        connection.disconnect()

        self._pharmacy_prices = result_table
        self._min_date = min(result_table['DateTime'])

        return True

    def _get_pharmacies_prices(self, pharmacies):
        if not pharmacies:
            return None

        settings = self.settings
        url_all_prices = settings.get_setting('prices_all_api')

        connection = ext_con.TabletkiAPI()

        cols = ['govcode', 'govid', 'innercode', 'price', 'priceReserve', 'ID_Branch']
        data = []
        res_df = pd.DataFrame(data, columns=cols)

        max_len = len(pharmacies)
        for id_pharmacy in pharmacies:
            index = pharmacies.index(id_pharmacy)

            code = self._as_code(id_pharmacy)
            url_all_prices_by_pharm = url_all_prices + '/?sn=' + str(code)

            df = connection.execute(url_all_prices_by_pharm, 'GET', {}, 'json_detailed')
            df['ID_Branch'] = id_pharmacy

            res_df = res_df.append(df, sort=False)

            print('Pharmacies: ' + str(index) + ' / ' + str(max_len))

        connection.disconnect()

        res_df.columns = ['GoodsCode', 'ID_Goods', 'InnerCode', 'Price', 'PriceReserve', 'ID_Branch']
        res_df['Price'] = res_df['Price'].astype(float)
        res_df['PriceReserve'] = res_df['PriceReserve'].astype(float)

        return res_df

    def _set_new_pharmacy_prices(self):
        if self.pharmacy_prices is None:
            return False

        goods = self.pharmacy_prices.ID_Goods
        data = []

        competitors_dict = {}
        prev_dist = 0
        distance_tuple = self.settings.get_setting('distances')
        for dist in distance_tuple:
            nearest_competitors = self._get_nearest_competitors(prev_dist, dist)
            competitors_dict[dist] = nearest_competitors
            prev_dist = dist

        max_len = len(goods)
        ind = 0
        for _, row in self.pharmacy_prices.iterrows():
            id_goods = row.ID_Goods
            inner_code = row.OuterCode
            name = row.Name
            producer = row.Producer
            price = row.Price
            new_price = row.PriceReserve
            quantity = row.Quantity

            ind += 1

            if ind % 10 == 0:
                print('Goods: ' + str(ind) + ' / ' + str(max_len))

            id_goods_no_link = '00000000-0000-0000-0000-000000000000'
            id_goods_link_not_needed = '50000000-0000-0000-0000-000000000000'
            if id_goods and id_goods not in (id_goods_no_link, id_goods_link_not_needed):
                curr_base_price = self._get_current_price(id_goods, 'base')
                curr_reserve_price = self._get_current_price(id_goods, 'reserve')
                new_price = self._get_new_price(id_goods, curr_base_price, curr_reserve_price, competitors_dict)

            row = [inner_code, name, producer, price, new_price, quantity]
            data.append(row)

        df = pd.DataFrame(data, columns=['Code', 'Name', 'Producer', 'Price', 'PriceReserve', 'Quantity'])
        self._new_prices = df

        return True

    def _get_price_range(self, price):
        if not self.settings:
            return 0

        price_ranges = self.settings.get_setting('prices')
        fit_range = [el for el in price_ranges if el > price]
        if fit_range:
            curr_range = fit_range[0]
        else:
            curr_range = price_ranges[-1]

        return curr_range

    def _get_new_price(self, id_goods, base_price, reserve_price, competitors_dict=None):
        if self.competitors_prices is None:
            return reserve_price

        if not base_price:
            base_price = reserve_price

        price_range = self._get_price_range(reserve_price)

        prices_df = self.competitors_prices
        slice_filter_by_goods = (prices_df['ID_Goods'] == id_goods)
        goods_prices_df = prices_df[slice_filter_by_goods]

        new_prices = []
        distance_tuple = self.settings.get_setting('distances')
        for dist in distance_tuple:
            nearest_competitors = competitors_dict[dist]

            slice_filter_by_competitors = goods_prices_df['ID_Branch'].isin(nearest_competitors)
            competitors_prices = goods_prices_df[slice_filter_by_competitors].Price
            if competitors_prices.empty:
                min_comp_price = 0
            else:
                min_comp_price = min(competitors_prices)

            ratio = self._get_ratio(price_range, dist)

            new_price = round(min_comp_price * ratio, 2)
            if new_price:
                new_prices.append(new_price)

        if not new_prices:
            min_new_price = base_price
        else:
            min_new_price = min(new_prices)

        max_reserve_price = max([reserve_price, min_new_price])
        final_price = min([base_price, max_reserve_price])

        return final_price

    def _get_current_price(self, id_goods, price_type='base'):
        prices = self.pharmacy_prices
        row = prices[(prices['ID_Goods'] == id_goods)]
        if row.empty:
            return 0

        if price_type == 'base':
            if isinstance(row.Price, pd.Series):
                price = max(row.Price)
            else:
                price = row.Price
        elif price_type == 'reserve':
            if isinstance(row.PriceReserve, pd.Series):
                price = max(row.PriceReserve)
            else:
                price = row.PriceReserve
        else:
            price = 0

        return price

    def _get_nearest_competitors(self, min_dist, max_dist):
        pharm_table = self.pharmacy_table

        enterprise_id = self._get_pharmacy_enterprise(self.id_pharmacy)
        competitors = pharm_table[pharm_table['ID_Enterprise'] != enterprise_id].ID_Branch

        dist_table = self.distance_table

        # Filtering all pharmacies for current one within distance range
        my_filter = dist_table[self.id_pharmacy].isin(range(int(min_dist), int(max_dist)))
        nearest_pharmacies = dist_table[my_filter][self.id_pharmacy]
        nearest_competitors = list(set(nearest_pharmacies.index).intersection(set(competitors)))

        return nearest_competitors

    def _get_pharmacy_enterprise(self, id_pharmacy):
        pharm_table = self.pharmacy_table
        curr_pharm_df = pharm_table[pharm_table['ID_Branch'] == id_pharmacy]
        if curr_pharm_df.empty:
            return ''

        enterprise_id = curr_pharm_df.iloc[0].ID_Enterprise

        return enterprise_id

    def _distance_between(self, id_pharmacy_1, id_pharmacy_2):
        return 0 if self.distance_table is None else self.distance_table[id_pharmacy_1][id_pharmacy_2]
