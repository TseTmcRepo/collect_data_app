from database import Database
from my_time import get_now_time_string


def transfer_new_records(source_db_data, destination_db_data, source_table_name,
                         destination_table_name, max_packet_size=4000000):

    source_db = Database(db_info=source_db_data)
    destination_db = Database(db_info=destination_db_data)

    # print('--------------- start : transfer_new_records at {0} -------------------'.format(get_now_time_string()))
    query = 'select count(*) from {0}'.format(source_table_name)
    args = ()
    source_data = source_db.select_query(query, args, True)
    if source_data is False:
        return 0, False
    source_table_record_count = int(source_data[0][0])

    # max_packet_size = 4000000
    if source_table_record_count > max_packet_size:
        loop_start = 0
        loop_offset = max_packet_size
        loop = 0
        while loop_start < source_table_record_count:
            loop += 1
            print('time: {0}:=> start get data from source loop:{1}'.format(get_now_time_string(), loop))
            query = 'select * from {0} limit {1}, {2}'.format(source_table_name, loop_start, loop_offset)
            args = ()
            source_data = source_db.select_query(query, args, True)
            if source_data is False:
                return 0, False
            print('time: {0}:=> end get data from source loop:{1}'.format(get_now_time_string(), loop))

            # انتقال داده ها در بسته های کوچکتر به جدول مقصد
            print('time: {0}:=> start transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))
            if len(source_data) > 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data)

                destination_db.command_query_many(query, args, True)

            elif len(source_data) == 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data[0])

                destination_db.command_query(query, args, True)

            print('time: {0}:=> end transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))

            loop_start += loop_offset
    else:
        # گرفتن تمام رکوردهای جدول مبدا
        print('time: {0}:=> start get data from source'.format(get_now_time_string()))
        query = 'select * from {0}'.format(source_table_name)
        args = ()
        source_data = source_db.select_query(query, args, True)
        if source_data is False:
            return 0, False

        print('time: {0}:=> end get data from source'.format(get_now_time_string()))

        if len(source_data) > 1:
            v = '%s, ' * len(source_data[0])
            v = v.strip(', ')
            query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
            args = list(source_data)

            destination_db.command_query_many(query, args, True)

        elif len(source_data) == 1:
            v = '%s, ' * len(source_data[0])
            v = v.strip(', ')
            query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
            args = list(source_data[0])

            destination_db.command_query(query, args, True)

    # print('--------------- end : transfer_new_records at {0} ------------------\n'.format(get_now_time_string()))
    return source_table_record_count, True

def transfer_new_records1(source_db_data, destination_db_data, source_table_name,
                         destination_table_name, max_packet_size=4000000):

    source_db = Database(db_info=source_db_data)
    destination_db = Database(db_info=destination_db_data)

    # print('--------------- start : transfer_new_records at {0} -------------------'.format(get_now_time_string()))
    query = 'select count(*) from {0}'.format(source_table_name)
    args = ()
    source_data = source_db.select_query(query, args, True)
    if source_data is False:
        return False
    source_table_record_count = int(source_data[0][0])

    # max_packet_size = 4000000
    if source_table_record_count > max_packet_size:
        loop_start = 0
        loop_offset = max_packet_size
        loop = 0
        while loop_start < source_table_record_count:
            loop += 1
            print('time: {0}:=> start get data from source loop:{1}'.format(get_now_time_string(), loop))
            query = 'select * from {0} limit {1}, {2}'.format(source_table_name, loop_start, loop_offset)
            args = ()
            source_data = source_db.select_query(query, args, True)
            if source_data is False:
                return False
            print('time: {0}:=> end get data from source loop:{1}'.format(get_now_time_string(), loop))

            # انتقال داده ها در بسته های کوچکتر به جدول مقصد
            print('time: {0}:=> start transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))
            if len(source_data) > 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data)

                destination_db.command_query_many(query, args, True)

            elif len(source_data) == 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data[0])

                destination_db.command_query(query, args, True)

            print('time: {0}:=> end transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))

            loop_start += loop_offset
    else:
        # گرفتن تمام رکوردهای جدول مبدا
        print('time: {0}:=> start get data from source'.format(get_now_time_string()))
        query = 'select * from {0}'.format(source_table_name)
        args = ()
        source_data = source_db.select_query(query, args, True)
        if source_data is False:
            return False

        print('time: {0}:=> end get data from source'.format(get_now_time_string()))

        if len(source_data) > 1:
            v = '%s, ' * len(source_data[0])
            v = v.strip(', ')
            query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
            args = list(source_data)

            destination_db.command_query_many(query, args, True)

        elif len(source_data) == 1:
            v = '%s, ' * len(source_data[0])
            v = v.strip(', ')
            query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
            args = list(source_data[0])

            destination_db.command_query(query, args, True)

    # print('--------------- end : transfer_new_records at {0} ------------------\n'.format(get_now_time_string()))
    return True


def transfer_new_records0(source_db_data, destination_db_data, source_table_name, destination_table_name):
    source_db = Database(db_info=source_db_data)
    destination_db = Database(db_info=destination_db_data)

    # print('--------------- start : transfer_new_records at {0} -------------------'.format(get_now_time_string()))
    query = 'select count(*) from {0}'.format(source_table_name)
    args = ()
    source_data = source_db.select_query(query, args, True)
    if source_data is False:
        return False
    source_table_record_count = int(source_data[0][0])

    max_packet_size = 4000000
    if source_table_record_count > max_packet_size:
        loop_start = 0
        loop_offset = max_packet_size
        loop = 0
        while loop_start < source_table_record_count:
            loop += 1
            print('time: {0}:=> start get data from source loop:{1}'.format(get_now_time_string(), loop))
            query = 'select * from {0} limit {1}, {2}'.format(source_table_name, loop_start, loop_offset)
            args = ()
            source_data = source_db.select_query(query, args, True)
            if source_data is False:
                return False
            print('time: {0}:=> end get data from source loop:{1}'.format(get_now_time_string(), loop))

            # انتقال داده ها در بسته های کوچکتر به جدول مقصد
            print('time: {0}:=> start transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))
            start = 0
            offset = 10000
            while start < len(source_data):
                print(start)
                sub = list(source_data[start:start + offset])
                start += offset

                values = '{0}'.format(sub)
                values = values.strip('[')
                values = values.strip(']')
                values = values.replace('\\u200c', '')
                values = values.replace('%', '%%')
                # ذخیره داده های بدست آمده در جدول مقصد در بلوکهایی به طول آفست
                query = 'insert IGNORE into {0} values '.format(destination_table_name) + values
                args = ()
                res = destination_db.command_query(query, args, True)
                if res is False:
                    return False
                sub.clear()
            print('time: {0}:=> end transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))

            loop_start += loop_offset
    else:
        # گرفتن تمام رکوردهای جدول مبدا
        print('time: {0}:=> start get data from source'.format(get_now_time_string()))
        query = 'select * from {0}'.format(source_table_name)
        args = ()
        source_data = source_db.select_query(query, args, True)
        if source_data is False:
            return False
        print('time: {0}:=> end get data from source'.format(get_now_time_string()))
        # print('time: {0}:=> all source row count:{1}'.format(get_now_time_string(), len(source_data)))

        # انتقال داده ها در بسته های کوچکتر به جدول مقصد
        print('time: {0}:=> end transfer data to destination table:{1}'.format(get_now_time_string(), len(source_data)))
        start = 0
        offset = 10000
        while start < len(source_data):
            print(start)
            sub = list(source_data[start:start + offset])
            start += offset

            values = '{0}'.format(sub)
            values = values.strip('[')
            values = values.strip(']')
            values = values.replace('\\u200c', '')
            values = values.replace('%', '%%')
            # ذخیره داده های بدست آمده در جدول مقصد در بلوکهایی به طول آفست
            query = 'insert IGNORE into {0} values '.format(destination_table_name) + values
            args = ()
            res = destination_db.command_query(query, args, True)
            if res is False:
                return False

            sub.clear()
        print('time: {0}:=> end transfer data to destination table:{1}'.format(get_now_time_string(), len(source_data)))
    # print('--------------- end : transfer_new_records at {0} ------------------\n'.format(get_now_time_string()))
    return True


def clean_table(source_db_data, source_table_name):
    source_db = Database(db_info=source_db_data)

    res = source_db.clean_table(source_table_name)
    if res is False:
        return False
    return True


def sync_table(source_db_info, destination_db_info, source_table_name, destination_table_name,
               pk_field, is_multi_pk, transaction_mod=False, destination_connection=None, max_packet_size=4000000):

    source_db = Database(db_info=source_db_info)
    destination_db = Database(db_info=destination_db_info)

    con = None
    if transaction_mod is True:
        if destination_connection is not None:
            con = destination_connection
        else:
            try:
                con, err = destination_db.get_connection()
                if err is not None:
                    raise Exception(err)

            except Exception as e:
                try:
                    if con.open is True:
                        con.rollback()
                        con.close()
                finally:
                    return 'cant create connection: {}'.format(str(e))


    # get destination pk records
    query = 'select {0} from {1} '.format(pk_field, destination_table_name)
    args = ()
    destination_pk = destination_db.select_query(query, args, True)
    if destination_pk is False:
        return False

    if is_multi_pk is False:
        l = list()
        for item in destination_pk:
            l.append(item[0])

        destination_pk = tuple(l)

    if len(destination_pk) > 0:
        if is_multi_pk is True:
            query = 'select count(*) from {0} where ({1}) not in {2}'.format(source_table_name, pk_field, destination_pk)
        else:
            query = 'select count(*) from {0} where {1} not in {2}'.format(source_table_name, pk_field, destination_pk)

    else:
        query = 'select count(*) from {0}'.format(source_table_name)

    args = ()
    new_record_count = source_db.select_query(query, args, True)
    if new_record_count is False:
        return False
    new_record_count = int(new_record_count[0][0])


    loop_start = 0
    loop_offset = max_packet_size
    loop = 0
    while loop_start < new_record_count:
        loop += 1
        print('time: {0}:=> start get data from source loop:{1}'.format(get_now_time_string(), loop))
        # query = 'select * from {0} where {1} not in {2} limit {3}, {4}'.format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)
        if len(destination_pk) > 0:
            if is_multi_pk is True:
                query = 'select * from {0} where ({1}) not in {2} order by {1} limit {3}, {4}'.format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)
            else:
                query = 'select * from {0} where {1} not in {2} order by {1} limit {3}, {4}'.format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)

        else:
            query = 'select * from {0} order by {1} limit {2}, {3}'.format(source_table_name, pk_field, loop_start, loop_offset)

        args = ()
        source_data = source_db.select_query(query, args, True)
        if source_data is False:
            return False
        print('time: {0}:=> end get data from source loop:{1}'.format(get_now_time_string(), loop))

        # انتقال داده ها در بسته های کوچکتر به جدول مقصد
        print('time: {0}:=> start transfer data to destination table loop:{1} count:{2}'
              .format(get_now_time_string(), loop, len(source_data)))
        if len(source_data) > 1:
            v = '%s, ' * len(source_data[0])
            v = v.strip(', ')
            query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
            args = list(source_data)

            # insert to destination
            if transaction_mod is False:
                destination_db.command_query_many(query, args, True)
            else:
                try:
                    db = con.cursor()
                    db._defer_warnings = True
                    db.autocommit = False
                    db.executemany(query, args)

                except Exception as e:
                    try:
                        if con.open is True:
                            con.rollback()
                            con.close()
                    finally:
                        return 'cant execute command_query_many: {}'.format(str(e))

        elif len(source_data) == 1:
            v = '%s, ' * len(source_data[0])
            v = v.strip(', ')
            query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
            args = list(source_data[0])

            # insert to destination
            if transaction_mod is False:
                destination_db.command_query(query, args, True)
            else:
                try:
                    db = con.cursor()
                    db._defer_warnings = True
                    db.autocommit = False
                    db.execute(query, args)

                except Exception as e:
                    try:
                        if con.open is True:
                            con.rollback()
                            con.close()
                    finally:
                        return 'cant execute command_query: {}'.format(str(e))

        print('time: {0}:=> end transfer data to destination table loop:{1} count:{2}'
              .format(get_now_time_string(), loop, len(source_data)))

        loop_start += loop_offset

    if transaction_mod is True and destination_connection is None:
        con.commit()
        con.close()

    return True


def sync_table0(source_db_info, destination_db_info, source_table_name, destination_table_name,
               pk_field, is_multi_pk, max_packet_size=4000000):

    source_db = Database(db_info=source_db_info)
    destination_db = Database(db_info=destination_db_info)


    # get destination pk records
    query = 'select {0} from {1} '.format(pk_field, destination_table_name)
    args = ()
    destination_pk = destination_db.select_query(query, args, True)
    if destination_pk is False:
        return False

    if is_multi_pk is False:
        l = list()
        for item in destination_pk:
            l.append(item[0])

        destination_pk = tuple(l)

    if len(destination_pk) > 0:
        if is_multi_pk is True:
            query = 'select count(*) from {0} where ({1}) not in {2}'.format(source_table_name, pk_field, destination_pk)
        else:
            query = 'select count(*) from {0} where {1} not in {2}'.format(source_table_name, pk_field, destination_pk)

    else:
        query = 'select count(*) from {0}'.format(source_table_name)

    args = ()
    new_record_count = source_db.select_query(query, args, True)
    if new_record_count is False:
        return False
    new_record_count = int(new_record_count[0][0])


    loop_start = 0
    loop_offset = max_packet_size
    loop = 0
    while loop_start < new_record_count:
        loop += 1
        print('time: {0}:=> start get data from source loop:{1}'.format(get_now_time_string(), loop))
        # query = 'select * from {0} where {1} not in {2} limit {3}, {4}'.format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)
        if len(destination_pk) > 0:
            if is_multi_pk is True:
                query = 'select * from {0} where ({1}) not in {2} order by {1} limit {3}, {4}'.format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)
            else:
                query = 'select * from {0} where {1} not in {2} order by {1} limit {3}, {4}'.format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)

        else:
            query = 'select * from {0} order by {1} limit {2}, {3}'.format(source_table_name, pk_field, loop_start, loop_offset)

        args = ()
        source_data = source_db.select_query(query, args, True)
        if source_data is False:
            return False
        print('time: {0}:=> end get data from source loop:{1}'.format(get_now_time_string(), loop))

        # انتقال داده ها در بسته های کوچکتر به جدول مقصد
        print('time: {0}:=> start transfer data to destination table loop:{1} count:{2}'
              .format(get_now_time_string(), loop, len(source_data)))
        if len(source_data) > 1:
            v = '%s, ' * len(source_data[0])
            v = v.strip(', ')
            query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
            args = list(source_data)

            destination_db.command_query_many(query, args, True)

        elif len(source_data) == 1:
            v = '%s, ' * len(source_data[0])
            v = v.strip(', ')
            query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
            args = list(source_data[0])

            destination_db.command_query(query, args, True)

        print('time: {0}:=> end transfer data to destination table loop:{1} count:{2}'
              .format(get_now_time_string(), loop, len(source_data)))

        loop_start += loop_offset

    return True


if __name__ == '__main__':
    from constant_database_data import *

    source_db_info = laptop_analyze_server_role_db_info
    destination_db_info = server_analyze_server_role_db_info

    max_packet_size = 4000000

    table_list = ['client_local_settings',
                  'excel_share_daily_data',
                  'fail_hang_share',
                  'fail_integrity_share',
                  'fail_other_share',
                  'fail_other_share',
                  'index_info',
                  'open_days',
                  'shareholders_data',
                  'share_daily_data',
                  'share_info',
                  'share_status',
                  'share_sub_trad_data'
                  ]

    backup_table_list = ['share_sub_trad_data',
                         'share_sub_trad_data',
                         'fail_other_share_backup',
                         'index_data_backup',
                         'shareholders_data_backup',
                         'share_daily_data_backup',
                         'share_info_backup',
                         'share_info_backup',
                         'share_sub_trad_data_backup'
                         ]

    analyzer_table = ['index_data',
                      'index_info',
                      'open_days',
                      'shareholders_data',
                      'share_adjusted_data',
                      'share_daily_data',
                      'share_info',
                      'share_second_data',
                      'share_status'
                     ]



    for table in analyzer_table:
        source_table_name = table
        destination_table_name = source_table_name
        print('start transfer table:{0}'.format(table))

        res = transfer_new_records(source_db_data=source_db_info, destination_db_data=destination_db_info,
                                   source_table_name=source_table_name, destination_table_name=destination_table_name,
                                   max_packet_size=max_packet_size)
        print(res)
