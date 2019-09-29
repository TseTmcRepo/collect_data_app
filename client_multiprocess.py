# from database import Database
from database import Database
from time import sleep
from functions import clean_table, transfer_new_records
# from constant_database_data import *
# from collect_trade_data_multi_thread import collect_trade_data_multi_thread
# from collect_data_subclass import collect_trade_data_multi_thread
from collect_data_multiprocessing_subclass import collect_trade_data_multi_process

from my_time import get_now_time_string

from termcolor import colored


class Client:
    def __init__(self, client_id, local_db_info, server_db_info, mod, backup_data_on_client=False):
        self.client_id = client_id
        self.local_db_info = local_db_info
        self.server_db_info = server_db_info
        self.mod = mod
        self.backup_data_on_client = backup_data_on_client

        self.local_db = Database(db_info=self.local_db_info)
        self.server_db = Database(db_info=self.server_db_info)

        self.setting = self.get_setting(True)
        if self.setting is False:
            return
        self.print_color = 'blue'

    # -------------------------
    def get_setting_from_server(self):
        return self.server_db.get_from_client_local_settings(self.client_id)

    def get_setting_from_local(self):
        return self.local_db.get_from_client_local_settings(self.client_id)

    def is_empty_db(self):
        return self.local_db.is_empty_db()

    def execute_integrity_rule(self):
        return True
        # self.local_db.clean_data_for_integrity()

    def transfer_data_to_server(self):
        sum_transfer = 0
        # fail_hang_share
        self.print_c('start update table: fail_hang_share')
        self.transfer_fail_hang_share()

        # fail_integrity_share
        self.print_c('start update table: fail_integrity_share')
        self.transfer_fail_integrity_share()

        # fail_other_share
        self.print_c('start update table: fail_other_share')
        self.transfer_fail_other_share()

        # index_data
        self.print_c('start update table: index_data')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='index_data',
                             destination_table_name='index_data')
        sum_transfer += res

        # index_info
        self.print_c('start update table: index_info')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='index_info',
                             destination_table_name='index_info')
        sum_transfer += res

        # open_days
        self.print_c('start update table: open_days')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='open_days',
                             destination_table_name='open_days')
        sum_transfer += res

        # share_daily_data
        self.print_c('start update table: share_daily_data')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='share_daily_data',
                             destination_table_name='share_daily_data')
        sum_transfer += res

        # share_adjusted_data
        self.print_c('start update table: share_adjusted_data')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='share_adjusted_data',
                             destination_table_name='share_adjusted_data')
        sum_transfer += res

        # share_info
        self.print_c('start update table: share_info')
        res, err = self.transfer_share_info()
        sum_transfer += res

        # share_second_data
        self.print_c('start update table: share_second_data')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='share_second_data',
                             destination_table_name='share_second_data')
        sum_transfer += res

        # share_status
        self.print_c('start update table: share_status')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='share_status',
                             destination_table_name='share_status')
        sum_transfer += res

        # share_sub_trad_data
        self.print_c('start update table: share_sub_trad_data')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='share_sub_trad_data',
                             destination_table_name='share_sub_trad_data')
        sum_transfer += res

        # shareholders_data
        self.print_c('start update table: shareholders_data')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='shareholders_data',
                             destination_table_name='shareholders_data')
        sum_transfer += res

        return sum_transfer, None

    def transfer_fail_integrity_share(self):
        # print('start update table: fail_integrity_share')

        query = 'select * from fail_integrity_share'
        args = ()
        source_data = self.local_db.select_query(query, args, True)
        if source_data is False:
            return False

        query = 'insert into fail_integrity_share (en_symbol_12_digit_code, date_m, fail_count) ' \
                'values (%s, %s, %s) ON DUPLICATE KEY UPDATE fail_count = fail_count + 1'

        args = list(source_data)
        if len(source_data) > 1:
            return self.server_db.command_query_many(query, args, True)

        elif len(source_data) == 1:
            args = list(source_data[0])

            return self.server_db.command_query(query, args, True)
        else:
            return True

    def transfer_fail_hang_share(self):
        #print('start update table: fail_hang_share')

        query = 'select * from fail_hang_share'
        args = ()
        source_data = self.local_db.select_query(query, args, True)
        if source_data is False:
            return False

        query = 'insert into fail_hang_share (en_symbol_12_digit_code, date_m, fail_count) ' \
                'values (%s, %s, %s) ON DUPLICATE KEY UPDATE fail_count = fail_count + 1'

        args = list(source_data)
        if len(source_data) > 1:
            return self.server_db.command_query_many(query, args, True)

        elif len(source_data) == 1:
            args = list(source_data[0])

            return self.server_db.command_query(query, args, True)
        else:
            return True

    def transfer_fail_other_share(self):
        # print('start update table: fail_other_share')

        query = 'select * from fail_other_share'
        args = ()
        source_data = self.local_db.select_query(query, args, True)
        if source_data is False:
            return False

        query = 'insert into fail_other_share (en_symbol_12_digit_code, tsetmc_id, date_m, fail_count) ' \
                'values (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE fail_count = fail_count + 1'

        args = list(source_data)
        if len(source_data) > 1:
            return self.server_db.command_query_many(query, args, True)

        elif len(source_data) == 1:
            args = list(source_data[0])

            return self.server_db.command_query(query, args, True)
        else:
            return True

    def transfer_share_info(self):
        # print('start update table: share_info')
        res, err = transfer_new_records(self.local_db_info, self.server_db_info, source_table_name='share_info',
                             destination_table_name='share_info')
        # update min max
        query = 'select en_symbol_12_digit_code, min_date, max_date from share_info'
        args = ()
        source_data = self.local_db.select_query(query, args, True)
        if source_data is False:
            return False
        for share in source_data:
            en_symbol_12_digit_code = share[0]
            start_accept_date = share[1]
            end_accept_date = share[2]

            # set_start_accept_date
            server_start_accept_date = self.server_db.get_start_accept_date(en_symbol_12_digit_code)
            if server_start_accept_date is not False:
                if start_accept_date > server_start_accept_date:
                    self.server_db.set_start_accept_date(en_symbol_12_digit_code, start_accept_date)

            # set_end_accept_date
            server_end_accept_date = self.server_db.get_end_accept_date(en_symbol_12_digit_code)
            if server_end_accept_date is not False:
                if end_accept_date < server_end_accept_date or (server_end_accept_date == 0 and end_accept_date > 0):
                    self.server_db.set_end_accept_date(en_symbol_12_digit_code, end_accept_date)
        return res, err

    # -----------
    def transfer_data_to_backup_tables(self):
        result = True
        # fail_hang_share
        res = self.backup_fail_hang_share()
        if res is not False:
            clean_table(self.local_db_info, 'fail_hang_share')

        # fail_integrity_share
        res = self.backup_fail_integrity_share()
        if res is not False:
            clean_table(self.local_db_info, 'fail_integrity_share')

        # fail_other_share
        res = self.backup_fail_other_share()
        if res is not False:
            clean_table(self.local_db_info, 'fail_other_share')

        # index_data
        # print('time: {0} :=> start backup share_status '.format(get_now_time_string()))
        # res = transfer_new_records(self.local_db_info, self.local_db_info, 'index_data', 'index_data_backup')
        #if res is not False:
        #    clean_table(self.local_db_info, 'index_data')

        # share_daily_data
        print('time: {0} :=> start backup share_daily_data'.format(get_now_time_string()))
        res, err = transfer_new_records(self.local_db_info, self.local_db_info, 'share_daily_data',
                                   'share_daily_data_backup')
        if err is not False:
            clean_table(self.local_db_info, 'share_daily_data')

        # share_status
        print('time: {0} :=> start backup share_status '.format(get_now_time_string()))
        res, err = transfer_new_records(self.local_db_info, self.local_db_info, 'share_status',
                                   'share_status_backup')
        if err is not False:
            clean_table(self.local_db_info, 'share_status')

        # share_sub_trad_data
        print('time: {0} :=> start backup share_sub_trad_data'.format(get_now_time_string()))
        res, err = transfer_new_records(self.local_db_info, self.local_db_info, 'share_sub_trad_data',
                                   'share_sub_trad_data_backup')
        if err is not False:
            clean_table(self.local_db_info, 'share_sub_trad_data')

        # shareholders_data
        print('time: {0} :=> start backup shareholders_data'.format(get_now_time_string()))
        res, err = transfer_new_records(self.local_db_info, self.local_db_info, 'shareholders_data',
                                   'shareholders_data_backup')
        if err is not False:
            clean_table(self.local_db_info, 'shareholders_data')

        # share_second_data
        print('time: {0} :=> start backup share_second_data'.format(get_now_time_string()))
        res, err = transfer_new_records(self.local_db_info, self.local_db_info, 'share_second_data',
                                   'share_second_data_backup')
        if err is not False:
            clean_table(self.local_db_info, 'share_second_data')

        # share_adjusted_data
        print('time: {0} :=> start backup share_adjusted_data'.format(get_now_time_string()))
        res, err = transfer_new_records(self.local_db_info, self.local_db_info, 'share_adjusted_data',
                                   'share_adjusted_data_backup')
        if err is not False:
            clean_table(self.local_db_info, 'share_adjusted_data')

        # share_info
        # print('time: {0} :=> start backup share_info'.format(get_now_time_string()))
        # res = transfer_new_records(self.local_db_info, self.local_db_info, 'share_info', 'share_info_backup')
        # if res is not False:
        #    clean_table(self.local_db_info, 'share_info')

    def backup_fail_integrity_share(self):
        # print('start update table: fail_integrity_share_backup')

        query = 'select * from fail_integrity_share'
        args = ()
        source_data = self.local_db.select_query(query, args, True)
        if source_data is False:
            return False

        query = 'insert into fail_integrity_share_backup (en_symbol_12_digit_code, date_m, fail_count) ' \
                'values (%s, %s, %s) ON DUPLICATE KEY UPDATE fail_count = fail_count + 1'

        args = list(source_data)
        if len(source_data) > 1:
            return self.local_db.command_query_many(query, args, True)

        elif len(source_data) == 1:
            args = list(source_data[0])

            return self.local_db.command_query(query, args, True)
        else:
            return True

    def backup_fail_hang_share(self):
        # print('start update table: fail_hang_share_backup')

        query = 'select * from fail_hang_share'
        args = ()
        source_data = self.local_db.select_query(query, args, True)
        if source_data is False:
            return False

        query = 'insert into fail_hang_share_backup (en_symbol_12_digit_code, date_m, fail_count) ' \
                'values (%s, %s, %s) ON DUPLICATE KEY UPDATE fail_count = fail_count + 1'

        args = list(source_data)
        if len(source_data) > 1:
            return self.local_db.command_query_many(query, args, True)

        elif len(source_data) == 1:
            args = list(source_data[0])

            return self.local_db.command_query(query, args, True)
        else:
            return True

    def backup_fail_other_share(self):
        # print('start update table: fail_other_share_backup')

        query = 'select * from fail_other_share'
        args = ()
        source_data = self.local_db.select_query(query, args, True)
        if source_data is False:
            return False

        query = 'insert into fail_other_share_backup (en_symbol_12_digit_code, tsetmc_id, date_m, fail_count) ' \
                'values (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE fail_count = fail_count + 1'

        args = list(source_data)
        if len(source_data) > 1:
            return self.local_db.command_query_many(query, args, True)

        elif len(source_data) == 1:
            args = list(source_data[0])

            return self.local_db.command_query(query, args, True)
        else:
            return True

    # -----------
    def update_client_database_from_server(self):
        # open_days
        print('start update table: open_days')
        transfer_new_records(self.server_db_info, self.local_db_info, source_table_name='open_days',
                             destination_table_name='open_days')

        # index_data
        # print('start update table: index_data')
        # transfer_new_records(self.server_db_info, self.local_db_info, source_table_name='index_data',
        #                     destination_table_name='index_data')

        # index_info
        # print('start update table: index_info')
        # transfer_new_records(self.server_db_info, self.local_db_info, source_table_name='index_info',
        #                     destination_table_name='index_info')

        # share_info
        print('start update table: share_info')
        self.transfer_share_info_from_server()
        # transfer_new_records(self.server_db_info, self.local_db_info, source_table_name='share_info',
        #                     destination_table_name='share_info')
        return True

    def transfer_share_info_from_server(self):
        print('start update table: share_info')
        transfer_new_records(self.server_db_info, self.local_db_info, source_table_name='share_info',
                             destination_table_name='share_info')
        # update min max
        query = 'select en_symbol_12_digit_code, min_date, max_date from share_info'
        args = ()
        source_data = self.server_db.select_query(query, args, True)
        if source_data is False:
            return False
        for share in source_data:
            en_symbol_12_digit_code = share[0]
            start_accept_date = share[1]
            end_accept_date = share[2]

            # set_start_accept_date
            local_start_accept_date = self.local_db.get_start_accept_date(en_symbol_12_digit_code)
            if local_start_accept_date is not False:
                if start_accept_date > local_start_accept_date:
                    self.local_db.set_start_accept_date(en_symbol_12_digit_code, start_accept_date)

            # set_end_accept_date
            local_end_accept_date = self.local_db.get_end_accept_date(en_symbol_12_digit_code)
            if local_end_accept_date is not False:
                if end_accept_date < local_end_accept_date or (local_end_accept_date == 0 and end_accept_date > 0):
                    self.local_db.set_end_accept_date(en_symbol_12_digit_code, end_accept_date)
        return True

    def save_local_setting_to_server(self):
        local_setting = self.local_db.get_from_client_local_settings(self.client_id)

        self.save_setting(local_setting)

    def get_wait_list_from_local(self, start_index, offset):
        return self.local_db.get_wait_list(start_index, offset)

    def get_wait_list_from_server(self, start_index, offset):
        return self.server_db.get_wait_list(start_index, offset)

    def save_setting(self, setting):
        try:
            #$self.local_db.set_to_client_local_setting(self.client_id, setting)
            self.server_db.set_to_client_local_setting(self.client_id, setting)
            return True
        except Exception as e:
            print('save_setting exception :{0}'.format(e))
            return False

    def collect_data(self, wait_list):
        collect_data_obj = collect_trade_data_multi_process(
            self.local_db_info, self.setting['max_thread'], wait_list, self.client_id)
        res = collect_data_obj.run()
        if res is False:
            collect_data_obj = None
            # sleep(20)
        return res

    def get_setting(self, is_new_loop):

        local_setting = self.get_setting_from_local()
        if local_setting is False:
            return False
        server_setting = self.get_setting_from_server()

        if server_setting is not False:  # بروز رسانی وضعیت اجرا
            local_setting['execute_status'] = server_setting['execute_status']
            local_setting['max_thread'] = server_setting['max_thread']
            local_setting['running_mod'] = server_setting['running_mod']
            local_setting['running_mod'] = local_setting['running_mod']
            local_setting['client_name'] = server_setting['client_name']
            if local_setting['running_mod'] == 'server':
                local_setting['first'] = server_setting['first']
                local_setting['end'] = server_setting['end']
                local_setting['offset'] = server_setting['offset']
                if is_new_loop is True:
                    local_setting['last'] = local_setting['first']
                else:
                    local_setting['last'] = server_setting['last']

                self.local_db.set_to_client_local_setting(self.client_id, local_setting)
                self.server_db.set_to_client_local_setting(self.client_id, local_setting)
        else:
            local_setting['running_mod'] = 'local'

        if local_setting['last'] >= local_setting['end']:
            print('client on finish loop : start new loop')
            local_setting['last'] = local_setting['first']

        return local_setting

    def get_setting_server_mod(self, is_new_loop):

        #$local_setting = self.get_setting_from_local()
        #$if local_setting is False:
        #$    return False
        server_setting = self.get_setting_from_server()
        local_setting = server_setting

        if server_setting is not False:  # بروز رسانی وضعیت اجرا
            #$local_setting['execute_status'] = server_setting['execute_status']
            #$local_setting['max_thread'] = server_setting['max_thread']
            #$local_setting['running_mod'] = server_setting['running_mod']
            local_setting['running_mod'] = 'server'
            #$local_setting['client_name'] = server_setting['client_name']
            if local_setting['running_mod'] == 'server':
                #$local_setting['first'] = server_setting['first']
                #$local_setting['end'] = server_setting['end']
                #$local_setting['offset'] = server_setting['offset']
                if is_new_loop is True:
                    local_setting['last'] = local_setting['first']
                #$else:
                #$    local_setting['last'] = server_setting['last']

                #open_day_count = self.server_db.get_open_day_count()
                #if open_day_count is not False:
                #    local_setting['end'] = open_day_count - 1

                #$self.local_db.set_to_client_local_setting(self.client_id, local_setting)
                self.server_db.set_to_client_local_setting(self.client_id, local_setting)
        else:
            local_setting['running_mod'] = 'local'

        if local_setting['last'] >= local_setting['end']:
            print('client on finish loop : start new loop')
            local_setting['last'] = local_setting['first']

        return local_setting

    def transfer_local_data_to_server(self):
        self.execute_integrity_rule()  # پاکسازی دیتابیس
        res, err = self.transfer_data_to_server()  # انتقال داده ها به سرور
        if err is not None:  # داده ها منتقل نشده اند
            self.print_c('cant transfer_data_to_server: {}'.format(err))
            return res, err
        else:
            if self.backup_data_on_client is True:
                self.transfer_data_to_backup_tables()  # انتقال اطلاعات به جدول پشتیبانی
            return res, None

    def get_database_record_count(self):
        return self.server_db.get_database_record()

    # -----------
    def collect_index_data(self, mod):
        from tsetmc import Tsetmc
        import threading
        from Log import Log_Mod
        from client_setting import local_db_info

        log_file_name = 'log.txt'
        log_table_name = 'bot_log'
        logging_mod = Log_Mod.console_file

        status = dict()
        lock = threading.Lock()
        wait_list = list()
        complete_list = list()
        running_list = list()
        fail_list = list()

        excel_path = 'C:\\Users\\Mostafa_Laptop\\Documents\\TseClient 2.0\\'
        id = 1

        a = Tsetmc(id=id, db_info=local_db_info, log_file_name=log_file_name,
                   log_table_name=log_table_name, logging_mod=logging_mod,
                   lock=lock, wait_list=wait_list, complete_list=complete_list, running_list=running_list,
                   fail_list=fail_list, status=status)

        error = a.get_all_index_daily_data(excel_path, mod=mod)
        return error

    def collect_share_info(self):
        from tsetmc import Tsetmc
        import threading
        from Log import Log_Mod
        from client_setting import local_db_info

        log_file_name = 'log.txt'
        log_table_name = 'bot_log'
        logging_mod = Log_Mod.console_file

        status = dict()
        lock = threading.Lock()
        wait_list = list()
        complete_list = list()
        running_list = list()
        fail_list = list()

        # excel_path = 'C:\\Users\\Mostafa_Laptop\\Documents\\TseClient 2.0\\'
        id = 1

        a = Tsetmc(id=id, db_info=local_db_info, log_file_name=log_file_name,
                   log_table_name=log_table_name, logging_mod=logging_mod,
                   lock=lock, wait_list=wait_list, complete_list=complete_list, running_list=running_list,
                   fail_list=fail_list, status=status)

        fail_readed_page, running_page, unreade_page, readed_pag = a.collect_all_shares_info()
        a.print_c(
            'fail_readed_page:{0}, running_page:{1}, unreade_page:{2}, readed_pag:{3}'.format(len(fail_readed_page),
                                                                                              len(running_page),
                                                                                              len(unreade_page),
                                                                                              len(readed_pag)))
        return len(readed_pag)

    # -----------
    def print_c(self, text, color=None):
        try:
            if color is None:
                print(colored(text, self.print_color))
            else:
                print(colored('| ', self.print_color) + colored(text, color))
        except Exception as e:
            #self.print_c(str(e), 'red')
           print(str(e))

    def run_slave_mod(self):
        new_loop = True
        max_loop = 3
        loop_number = 1
        last = -1
        sum_new_record = 0
        while True:
            self.print_c('get setting')
            self.setting = self.get_setting(is_new_loop=new_loop)

            if last > self.setting['last']:
                loop_number += 1
            print(last)
            print(loop_number)
            last = self.setting['last']
            print(last)

            self.print_c('check exit condition')
            #  exit condition
            if self.setting['execute_status'] > 0:
                self.print_c('exit client from user')
                break  # exit function

            if loop_number > max_loop:
                self.print_c('exit client in max loop')
                break  # exit function

            if self.setting['running_mod'] == 'server':
                self.print_c('server mod')

                # transfer old data
                # self.transfer_local_data_to_server()
                res, err = self.transfer_local_data_to_server()
                sum_new_record += res

                #self.print_c('check old data')
                #if self.is_empty_db() is False:  # داده قدیمی دارد
                #    self.print_c('have old date in database')
                #    if self.transfer_local_data_to_server() is False:
                #        continue

                #  update local database from server
                self.print_c('check update database')
                if new_loop is True:
                    self.print_c('update client date list')
                    if self.update_client_database_from_server() is False:  # بروزرسانی اطلاعات روزهای معانلاتی و نمادها
                        continue

                if new_loop is True:
                    new_loop = False

                #  get wait list from server
                self.print_c('get wait list')
                if self.setting['last'] + self.setting['offset'] > self.setting['end']:
                    offset = self.setting['end'] - self.setting['first']
                else:
                    offset = self.setting['last'] + self.setting['offset'] - self.setting['first']
                wait_list = self.get_wait_list_from_server(self.setting['first'], offset)  # گرتن لیست روز نماد از سرور
                if wait_list is False:
                    self.print_c('cant get wait list from server')
                    continue

                self.setting['last'] = self.setting['first'] + offset
                self.print_c('save setting')
                self.save_setting(self.setting)

                if len(wait_list) > 0:
                    self.print_c('collect data')
                    self.collect_data(wait_list)  # جمع آوری اطلاعات

                    #  transfer old data
                    self.print_c('transfer data to server')
                    res, err = self.transfer_local_data_to_server()
                    sum_new_record += res

            else:
                self.print_c('local mod')
                self.print_c('check old data')
                if self.is_empty_db() is False:  # داده قدیمی دارد
                    self.print_c('have old date in database')
                    self.execute_integrity_rule()  # پاکسازی دیتابیس

                self.print_c('get wait list')
                if self.setting['last'] + self.setting['offset'] > self.setting['end']:
                    offset = self.setting['end'] - self.setting['last']
                else:
                    offset = self.setting['offset']

                wait_list = self.get_wait_list_from_local(self.setting['last'], offset)  # گرفتن لیست روز نماد از لوکال
                # self.print_c('start {0}, offset {1}'.format(self.setting['last'], offset))
                if wait_list is False:
                    print('cant get wait list from local')
                    continue
                self.setting['last'] += offset
                # self.print_c('start {0}, offset {1}'.format(self.setting['last'], offset))

                self.print_c('save setting')
                self.save_setting(self.setting)

                if len(wait_list) > 0:
                    self.print_c('collect data')
                    self.collect_data(wait_list)  # جمع آوری اطلاعات

            self.print_c('save setting to server')
            self.save_local_setting_to_server()
            # sleep(3)

        return sum_new_record

    def run_master_mod(self):
        new_loop = True
        max_loop = 3
        loop_number = 1
        last = -1
        sum_new_record = 0
        while True:
            self.print_c('get setting')
            self.setting = self.get_setting_server_mod(is_new_loop=new_loop)

            #$    if last >= self.setting['last']:
            if last > self.setting['last']:
                loop_number += 1
            print(last)
            print(loop_number)
            last = self.setting['last']
            print(last)

            self.print_c('check exit condition')
            #  exit condition
            if self.setting['execute_status'] > 0:
                self.print_c('exit client from user')
                break  # exit function

            if loop_number > max_loop:
                self.print_c('exit client in max loop')
                break  # exit function

            if self.setting['running_mod'] == 'server':
                self.print_c('server mod')

                # transfer old data
                # self.transfer_local_data_to_server()
                #$res, err = self.transfer_local_data_to_server()
                #$sum_new_record += res

                #self.print_c('check old data')
                #if self.is_empty_db() is False:  # داده قدیمی دارد
                #    self.print_c('have old date in database')
                #    if self.transfer_local_data_to_server() is False:
                #        continue

                #  update local database from server
                #$self.print_c('check update database')
                #$if new_loop is True:
                #$    self.print_c('update client date list')
                #$    if self.update_client_database_from_server() is False:  # بروزرسانی اطلاعات روزهای معانلاتی و نمادها
                #$        continue

                if new_loop is True:
                    new_loop = False

                #  get wait list from server
                self.print_c('get wait list')
                if self.setting['last'] + self.setting['offset'] > self.setting['end']:
                    offset = self.setting['end'] - self.setting['first']
                else:
                    offset = self.setting['last'] + self.setting['offset'] - self.setting['first']
                wait_list = self.get_wait_list_from_server(self.setting['first'], offset)  # گرتن لیست روز نماد از سرور
                if wait_list is False:
                    self.print_c('cant get wait list from server')
                    continue

                self.setting['last'] = self.setting['first'] + offset
                self.print_c('save setting')
                self.save_setting(self.setting)

                if len(wait_list) > 0:
                    self.print_c('collect data')
                    # گرفتن تعداد رکوردهای دیتابیس
                    before_record_count, err = self.get_database_record_count()
                    self.collect_data(wait_list)  # جمع آوری اطلاعات
                    # گرفتن تعداد رکوردهای دیتابیس
                    after_record_count, err = self.get_database_record_count()

                    if before_record_count > after_record_count:
                        sum_new_record += (before_record_count - after_record_count)
                    else:
                        sum_new_record += (after_record_count - before_record_count)

                    #  transfer old data
                    self.print_c('transfer data to server')
                    print(before_record_count)
                    print(after_record_count)
                    print(sum_new_record)
                    #$res, err = self.transfer_local_data_to_server()
                    #$sum_new_record += res

            else:
                self.print_c('local mod')
                #$self.print_c('check old data')
                #$if self.is_empty_db() is False:  # داده قدیمی دارد
                #$   self.print_c('have old date in database')
                #$   self.execute_integrity_rule()  # پاکسازی دیتابیس

                #$self.print_c('get wait list')
                #$if self.setting['last'] + self.setting['offset'] > self.setting['end']:
                #$    offset = self.setting['end'] - self.setting['last']
                #$else:
                #$    offset = self.setting['offset']

                #$wait_list = self.get_wait_list_from_local(self.setting['last'], offset)  # گرفتن لیست روز نماد از لوکال
                # self.print_c('start {0}, offset {1}'.format(self.setting['last'], offset))
                #$if wait_list is False:
                #$    print('cant get wait list from local')
                #$    continue
                #$self.setting['last'] += offset
                # self.print_c('start {0}, offset {1}'.format(self.setting['last'], offset))

                #$self.print_c('save setting')
                #$self.save_setting(self.setting)

                #$if len(wait_list) > 0:
                #$    self.print_c('collect data')
                #$    self.collect_data(wait_list)  # جمع آوری اطلاعات

            #$self.print_c('save setting to server')
            #$self.save_local_setting_to_server()
            # sleep(3)

        return sum_new_record

    def set_database_update_time(self, update_time):
        return  self.server_db.set_database_update_time(update_time)

    # -------------------------
    def run(self):
        if self.mod == 'slave':
            return self.run_slave_mod()
        elif self.mod == 'master':
            return self.run_master_mod()
        else:
            return -1

if __name__ == '__main__':
    import client_setting

    client_id = client_setting.client_id
    cli = Client(client_id=client_id, local_db_info=client_setting.local_db_info,
                 server_db_info=client_setting.local_db_info, mod='master')
                 #server_db_info=client_setting.server_db_info)

    # cli.update_client_database_from_server()
    # cli.transfer_share_info()



    # cli.collect_index_data(mod=1)
    # cli.collect_share_info()
    res = cli.run()
    #res = cli.run()
    print(res)
