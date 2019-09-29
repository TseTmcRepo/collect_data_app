import client_setting
from client_multiprocess import Client
from my_time import get_now_time_second, get_now_time_string, get_now_time_datetime
from time import sleep


class CollectDataServer:
    def __init__(self, client_id, cycle_period_time, local_db_info, server_db_info, mod, steps=7):
        self.client_id = client_id
        self.cycle_period_time = cycle_period_time
        self.local_db_info = local_db_info
        self.server_db_info = server_db_info
        self.mod = mod
        self.steps = steps

        if self.steps in [1, 3, 5, 7]:
            self.step_collect_share_info = True
        else:
            self.step_collect_share_info = False

        if self.steps in [2, 3, 6, 7]:
            self.step_collect_index_data = True
        else:
            self.step_collect_index_data = False

        if self.steps in [4, 5, 6, 7]:
            self.step_collect_share_all_data = True
        else:
            self.step_collect_share_all_data = False

        if self.mod == 'master':
            self.server_db_info = self.local_db_info

        self.cli = Client(client_id=self.client_id, local_db_info=client_setting.local_db_info,
                          server_db_info=client_setting.server_db_info, mod=self.mod)

        hour = 18
        minute = 0
        sec = 0
        self.tsetmc_update_time = hour * 10000 + minute * 100 + sec

    def start(self):
        last_collect_share_info_time = 0
        last_collect_index_data_time = 0
        last_collect_trade_data_time = 0
        have_new_data = False

        print(self.step_collect_share_info)
        print(self.step_collect_index_data)
        print(self.step_collect_share_all_data)

        while True:
            start_cycle_time = get_now_time_second()

            # ---------------------------------------------
            # update symbol info
            now_time = get_now_time_datetime()

            if last_collect_share_info_time == 0:
                if self.step_collect_share_info is True:
                    res = self.cli.collect_share_info()
                else:
                    res = 0

                if res > 0:
                    last_collect_share_info_time = get_now_time_datetime()
                    have_new_data = True
            else:
                if now_time.day in [1, 11, 21] and (last_collect_share_info_time.year != now_time.year or
                                                    last_collect_share_info_time.month != now_time.month or
                                                    last_collect_share_info_time.day != now_time.day):
                    res = self.cli.collect_share_info()
                    if res > 0:
                        last_collect_share_info_time = get_now_time_datetime()
                        have_new_data = True

            # ---------------------------------------------
            # update index daily date
            int_now_time = now_time.hour * 10000 + now_time.minute * 100 + now_time.second
            if last_collect_index_data_time == 0:
                if self.step_collect_index_data is True:
                    res = self.cli.collect_index_data(mod=1)
                else:
                    res = 0

                # res = None
                if res is None:
                    last_collect_index_data_time = get_now_time_datetime()
                    have_new_data = True

            else:
                if int_now_time > self.tsetmc_update_time and (now_time.year != last_collect_index_data_time.year or
                                                               now_time.month != last_collect_index_data_time.month or
                                                               now_time.day != last_collect_index_data_time.day):
                    res = self.cli.collect_index_data(mod=1)
                    if res is None:
                        last_collect_index_data_time = get_now_time_datetime()
                        have_new_data = True

            # ---------------------------------------------
            # update trade data
            int_now_time = now_time.hour * 10000 + now_time.minute * 100 + now_time.second
            if last_collect_trade_data_time == 0:
                if self.step_collect_share_all_data is True:
                    res = self.cli.run()
                else:
                    res = 0

                if res > 0:
                    last_collect_trade_data_time = get_now_time_datetime()
                    have_new_data = True

            else:
                if int_now_time > self.tsetmc_update_time and (now_time.year != last_collect_trade_data_time.year or
                                                               now_time.month != last_collect_trade_data_time.month or
                                                               now_time.day != last_collect_trade_data_time.day):
                    res = self.cli.run()
                    if res > 0:
                        last_collect_trade_data_time = get_now_time_datetime()
                        have_new_data = True

            # ---------------------------------------------
            if have_new_data is True:  # have new record
                # save database update time
                self.cli.set_database_update_time(start_cycle_time)
                have_new_data = False

            sleep_time = start_cycle_time + self.cycle_period_time - get_now_time_second()

            if sleep_time > 0:
                print('sleep sync process. start sleep: {0} sleep_time: {1}'.format(get_now_time_string(), sleep_time))
                sleep(sleep_time)


def temp():
    cycle_period_time = 60 * 1

    client_id = client_setting.client_id
    cli = Client(client_id=client_id, local_db_info=client_setting.local_db_info,
                 server_db_info=client_setting.server_db_info, mod='')

    # cli.update_client_database_from_server()
    # cli.transfer_share_info()

    # cli.collect_index_data(mod=1)
    # cli.collect_share_info()
    # res = cli.run()
    # print(res)

    hour = 18
    minute = 0
    sec = 0
    tsetmc_update_time = hour * 10000 + minute * 100 + sec

    last_collect_share_info_time = 0
    last_collect_index_data_time = 0
    last_collect_trade_data_time = 0
    have_new_data = False

    while True:
        start_cycle_time = get_now_time_second()

        # ---------------------------------------------
        # update symbol info
        now_time = get_now_time_datetime()

        if last_collect_share_info_time == 0:
            res = cli.collect_share_info()
            # res = 10
            if res > 0:
                last_collect_share_info_time = get_now_time_datetime()
                have_new_data = True
        else:
            if now_time.day in [1, 11, 21] and (last_collect_share_info_time.year != now_time.year or
                                                last_collect_share_info_time.month != now_time.month or
                                                last_collect_share_info_time.day != now_time.day):
                res = cli.collect_share_info()
                if res > 0:
                    last_collect_share_info_time = get_now_time_datetime()
                    have_new_data = True

        # ---------------------------------------------
        # update index daily date
        int_now_time = now_time.hour * 10000 + now_time.minute * 100 + now_time.second
        if last_collect_index_data_time == 0:
            res = cli.collect_index_data(mod=1)
            # res = None
            if res is None:
                last_collect_index_data_time = get_now_time_datetime()
                have_new_data = True

        else:
            if int_now_time > tsetmc_update_time and (now_time.year != last_collect_index_data_time.year or
                                                      now_time.month != last_collect_index_data_time.month or
                                                      now_time.day != last_collect_index_data_time.day):
                res = cli.collect_index_data(mod=1)
                if res is None:
                    last_collect_index_data_time = get_now_time_datetime()
                    have_new_data = True

        # ---------------------------------------------
        # update trade data
        int_now_time = now_time.hour * 10000 + now_time.minute * 100 + now_time.second
        if last_collect_trade_data_time == 0:
            res = cli.run()
            # res = 10
            if res > 0:
                last_collect_trade_data_time = get_now_time_datetime()
                have_new_data = True

        else:
            if int_now_time > tsetmc_update_time and (now_time.year != last_collect_trade_data_time.year or
                                                      now_time.month != last_collect_trade_data_time.month or
                                                      now_time.day != last_collect_trade_data_time.day):
                res = cli.run()
                if res > 0:
                    last_collect_trade_data_time = get_now_time_datetime()
                    have_new_data = True

        # ---------------------------------------------
        if have_new_data is True:  # have new record
            # save database update time
            cli.set_database_update_time(start_cycle_time)
            have_new_data = False

        sleep_time = start_cycle_time + cycle_period_time - get_now_time_second()

        if sleep_time > 0:
            print('sleep sync process. start sleep: {0} sleep_time: {1}'.format(get_now_time_string(), sleep_time))
            sleep(sleep_time)


if __name__ == '__main__':
    server = CollectDataServer(client_id=client_setting.client_id, cycle_period_time=60 * 10,
                               local_db_info=client_setting.local_db_info, server_db_info=client_setting.server_db_info,
                               mod='master', steps=4)

    server.start()
