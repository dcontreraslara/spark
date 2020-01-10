import copy
from datetime import datetime, timedelta

# data = [
#     ["prod1", datetime(2017, 9, 8, 1, 13, 45), datetime(2018, 9, 15, 14, 14, 41), ""],
#     ["prod1", datetime(2017, 9, 8, 1, 13, 45), datetime(2999, 12, 31, 0, 0, 0), ""],
#     ]
from tabulate import tabulate

data = [
    ["TRYAN", datetime(2018, 7, 28, 0, 33, 55), datetime(2018, 9, 26, 10, 33, 55), ""],
    ["TVEPR", datetime(2018, 8, 16, 2, 39, 9), datetime(2019, 4, 1, 1, 35, 42), ""],
    ["TVEBA", datetime(2018, 8, 16, 2, 39, 9), datetime(9999, 12, 31, 2, 39, 9), ""],
    ]

data = [
    ["TRYAN", datetime(2019, 9, 8, 1, 13, 45), datetime(2019, 9, 12, 1, 13, 44), ""],
    ["TVEPR", datetime(2019, 9, 9, 1, 13, 45), datetime(2019, 9, 13, 1, 13, 44), ""],
    ]

first = True
group_output = list()
account_result_list = list()
end_date_col_idx = 2
start_date_col_idx = 1
group_status_str_col_idx = 0
max_end = datetime(4000, 12, 31)
secs_offset = 1
sorted_group_status_str_col_idx = 3
name_sep = ","

for current_record in data:
    if first:
        current_record[sorted_group_status_str_col_idx] = current_record[group_status_str_col_idx]
        group_output.append(current_record)
        first = False
        max_end = current_record[end_date_col_idx]
        continue

    if current_record[start_date_col_idx] > max_end:
        account_result_list += group_output
        del group_output[:]
        group_output.append(current_record)
        max_end = max(max_end, current_record[end_date_col_idx])
        continue

    need_another_loop = True
    secs_offset = 1

    aux = copy.copy(current_record)
    while need_another_loop:
        cnt = 0
        print("----------")
        init_total = len(group_output) - 1
        for i, old_record in enumerate(group_output):
            # we overlap
            print('--->', aux, old_record[start_date_col_idx], old_record[end_date_col_idx], old_record[0])
            need_another_loop = False
            if old_record[start_date_col_idx] <= aux[start_date_col_idx] <= old_record[end_date_col_idx]:
                print (",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,, remove ", old_record)
                group_output.remove(old_record)
                # NEW RECORD 1
                # same start not needed record 1, do not need candidate, old start < new start
                if old_record[start_date_col_idx] != aux[start_date_col_idx]:
                    new_record_1 = copy.copy(old_record)
                    new_record_1[end_date_col_idx] = aux[start_date_col_idx] + timedelta(seconds=-secs_offset)
                    group_output.append(new_record_1)
                    cnt += 1
                    print("adds r1", new_record_1)

                # NEW RECORD 2
                # we need another loop if current record end is major than the old record
                new_record_2 = copy.copy(aux)
                new_record_2[end_date_col_idx] = min(aux[end_date_col_idx] , old_record[end_date_col_idx])
                prod_names_list = set(new_record_2[group_status_str_col_idx].split('-'))
                prod_names_list.add(old_record[group_status_str_col_idx])
                sorted_prod_names_list = sorted(prod_names_list)
                new_record_2[sorted_group_status_str_col_idx] = name_sep.join(sorted_prod_names_list)
                new_record_2[group_status_str_col_idx] = name_sep.join(sorted_prod_names_list)
                group_output.append(new_record_2)
                print("adds r2", new_record_2)

                # if old record end is less than new , we should create a new aux in order to eval the rest for future.
                if old_record[end_date_col_idx] < aux[end_date_col_idx]:
                    aux[start_date_col_idx] = old_record[end_date_col_idx] + timedelta(seconds=secs_offset)
                    if cnt >= init_total:
                        group_output.append(aux)
                        need_another_loop = False
                    else:
                        need_another_loop = True
                    print("continue -> new aux", aux, need_another_loop)
                elif old_record[end_date_col_idx] > aux[end_date_col_idx]:
                    new_record_3 = copy.copy(old_record)
                    new_record_3[start_date_col_idx] = min(aux[end_date_col_idx], old_record[end_date_col_idx])
                    new_record_3[start_date_col_idx] += timedelta(seconds=secs_offset)
                    group_output.append(new_record_3)
                    print("adds r3", new_record_3)

                max_end = max(max_end, aux[end_date_col_idx])
                break
        # next item in aux, is major than every end date
        if max(group_output, key=lambda tup: tup[end_date_col_idx])[end_date_col_idx] < aux[start_date_col_idx]:
            group_output.append(aux)
            break

account_result_list += group_output
account_result_list  = sorted(account_result_list , key=lambda tup: tup[1])
for i in account_result_list:
    print (i[1].strftime("%Y-%m-%d %H:%M:%S") , i[2].strftime("%Y-%m-%d %H:%M:%S"), i[0])
