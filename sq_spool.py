from os import system, name
import time
import sqlite3
import math
from itertools import zip_longest

barn_list = []
pull_list = []
cam_pull_list = []
b_check_list = []
cpp_check_list = []

gc_barn_list = []
gc_pull_list = []
gc_cam_pull_list = []

catsix_spool_check = {}
gc_spool_check = {}

conn = sqlite3.connect('farm.db')

c = conn.cursor()
c2 = conn.cursor()

CATsix_table = ("""CREATE TABLE IF NOT EXISTS
catsix (barn_id INTEGER, pull_id INTEGER, cameras_per_pull INTEGER, camera_id INTEGER, camera_length INTEGER, spool_id INTEGER)""")

c.execute(CATsix_table)

GC_table = ("""CREATE TABLE IF NOT EXISTS
gc (barn_id INTEGER, pull_id INTEGER, cameras_per_pull INTEGER, camera_id INTEGER, camera_length INTEGER, spool_id INTEGER)""")

c.execute(GC_table)

Barn_pull_table = ("""CREATE TABLE IF NOT EXISTS
bp (barn_id INTEGER, pull_id INTEGER, camera_id INTEGER, check_id INTEGER)""")

c.execute(Barn_pull_table)

GC_barn_pull_table = ("""CREATE TABLE IF NOT EXISTS
gc_bp (barn_id INTEGER, pull_id INTEGER, camera_id INTEGER, check_id INTEGER)""")

c.execute(GC_barn_pull_table)

all_catsix = "SELECT * FROM catsix"
all_gc = "SELECT * FROM gc"

def get_pulls(barn, pull, camera_id):
    c.execute("INSERT INTO bp VALUES (:barn_id, :pull_id, :camera_id, :check_id)", (barn, pull, camera_id, 0))
    
def get_gc_pulls(barn, pull, camera_id):
    c.execute("INSERT INTO gc_bp VALUES (:barn_id, :pull_id, :camera_id, :check_id)", (barn, pull, camera_id, 0))

def compare_database(database):
    c.execute(database)
    result = c.fetchone()
    row_count = result[0]
    return row_count

def spool_creator(total_length, dict_name):
    c.execute(total_length)
    total_l = c.fetchone()
    if total_l[0] > 1000:
        est_spools = total_l[0] / 1000
        up_est_spools = math.ceil(est_spools)
        for i in range(1, up_est_spools+1):
            spool_num = i
            dict_name[spool_num] = 0
    else:
        dict_name[1] = 0
        
def add_spools(num_add, dict_name):
    num_spools = len(dict_name)
    for add_spool in range(num_add):
        num_spools += 1
        spool_num = num_spools
        dict_name[spool_num] = 0
        return spool_num

def update_spool_dict(spool_id, spool_len_used, dict_name):
    for spool_num, length in dict_name.items():
        if spool_num == spool_id:
            update_spool = length + spool_len_used
            dict_name[spool_num] = update_spool

def get_spool_amount(spool_id, dict_name):
    for spool_num, length in dict_name.items():
        if spool_num == spool_id:
            spool_amount_l = 1000 - length
            return spool_amount_l

def get_spool(cam_length, ignore, dict_name):
    for spool_num, length in dict_name.items():
        spool_amount_l = 1000 - length
        if spool_num == ignore:
            pass
        else:
            if spool_amount_l >= cam_length:
                return spool_num 

def max_camera_pull(num_spools, max_spool_qty, dict_name):
    if num_spools < max_spool_qty:
        spool_diff = max_spool_qty - num_spools
        add_spools(spool_diff, dict_name)

def bp_db_check(db_name, b_check, p_check, c_check):
    c.execute(f"SELECT * FROM {db_name}")
    records = c.fetchall()
    for row in records:
        if row[0] == b_check:
            if row[1] == p_check:
                if row[2] == c_check:
                    check_status = row[3]
                    return check_status 
                
def update_bp_db(db_name, b_check, p_check, c_check):
    c.execute(f"SELECT * FROM {db_name}")
    records = c.fetchall()
    for row in records:
        if row[0] == b_check:
            if row[1] == p_check:
                if row[2] == c_check:
                    c.execute(f"UPDATE {db_name} SET check_id='1' WHERE barn_id={row[0]} and pull_id={row[1]} and camera_id={row[2]}")

def clear():
    if name == "nt":
        _ = system("cls")
    else:
        _ = system("clear")

def display_results(cable_db, spool):
    c.execute(cable_db)
    records = c.fetchall()
    print(f"SPOOL {spool}")
    print("-------\n")
    for row in records:
         print(f"Barn {row[0]} | Camera {row[3]} | Length: {row[4]} ft") 
    print("\n")

def finish_barns(next_barn, db_name, check_db, dict_name):
    c.execute(next_barn)
    records = c.fetchall()
    pull_list_check_one = []
    pull_list_check_two = []
    for row in records:
        b_check = row[0] 
        p_check = row[1]
        cp_check = row[2]
        c_check = row[3]
        l_check = row[4]
        bp_check = bp_db_check(check_db, b_check, p_check, c_check)
        if 0 == bp_check:
            if p_check == 1:
                try:
                    get_spool_num = get_spool(l_check, 0, dict_name)
                    if get_spool_num not in pull_list_check_one: 
                        assert(get_spool_num)
                        pull_list_check_one.append(get_spool_num)
                        update_spool_dict(get_spool_num, l_check, dict_name)
                        c.execute(f"UPDATE {db_name} SET spool_id={get_spool_num} WHERE barn_id={b_check} and pull_id={p_check} and cameras_per_pull={cp_check} and camera_id={c_check} and camera_length={l_check} and spool_id='0'")
                        update_bp_db(check_db, b_check, p_check, c_check)
                    else:
                        spool_num = get_spool(l_check, get_spool_num, dict_name)
                        assert(spool_num)
                        pull_list_check_one.append(spool_num)
                        update_spool_dict(spool_num, l_check, dict_name)
                        c.execute(f"UPDATE {db_name} SET spool_id={spool_num} WHERE barn_id={b_check} and pull_id={p_check} and cameras_per_pull={cp_check} and camera_id={c_check} and camera_length={l_check} and spool_id='0'")
                        update_bp_db(check_db, b_check, p_check, c_check)
                except:
                    next_spool_num = add_spools(1, dict_name)
                    pull_list_check_one.append(next_spool_num)
                    update_spool_dict(next_spool_num, l_check, dict_name)
                    c.execute(f"UPDATE {db_name} SET spool_id={next_spool_num} WHERE barn_id={b_check} and pull_id={p_check} and cameras_per_pull={cp_check} and camera_id={c_check} and camera_length={l_check} and spool_id='0'")
                    update_bp_db(check_db, b_check, p_check, c_check)
            elif p_check == 2:
                try:
                    get_spool_num = get_spool(l_check, 0, dict_name)
                    if get_spool_num not in pull_list_check_two: 
                        assert(get_spool_num)
                        pull_list_check_two.append(get_spool_num)
                        update_spool_dict(get_spool_num, l_check, dict_name)
                        c.execute(f"UPDATE {db_name} SET spool_id={get_spool_num} WHERE barn_id={b_check} and pull_id={p_check} and cameras_per_pull={cp_check} and camera_id={c_check} and camera_length={l_check} and spool_id='0'")
                        update_bp_db(check_db, b_check, p_check, c_check)
                    else:
                        spool_num = get_spool(l_check, get_spool_num, dict_name)
                        assert(spool_num)
                        pull_list_check_two.append(spool_num)
                        update_spool_dict(spool_num, l_check, dict_name)
                        c.execute(f"UPDATE {db_name} SET spool_id={spool_num} WHERE barn_id={b_check} and pull_id={p_check} and cameras_per_pull={cp_check} and camera_id={c_check} and camera_length={l_check} and spool_id='0'")
                        update_bp_db(check_db, b_check, p_check, c_check)
                except:
                    next_spool_num = add_spools(1, dict_name)
                    pull_list_check_two.append(next_spool_num)
                    update_spool_dict(next_spool_num, l_check, dict_name)
                    c.execute(f"UPDATE {db_name} SET spool_id={next_spool_num} WHERE barn_id={b_check} and pull_id={p_check} and cameras_per_pull={cp_check} and camera_id={c_check} and camera_length={l_check} and spool_id='0'")
                    update_bp_db(check_db, b_check, p_check, c_check)
                    
    pull_list_check_one.clear()
    pull_list_check_two.clear()

def biggest_pull(max_pull, b_list, check_db, db_name, max_spools, dict_name):      # finds one of the biggest pull and creates enough spools and also makes sure it checks the one it assigns
    c.execute(max_pull)
    records = c.fetchall()
    barn_ids = records[0][0]
    pull_ids = records[0][1]
    num = 0
    for row in records:
        num += 1
        dict_name[num] = row[4]
        c.execute(f"UPDATE {db_name} SET spool_id={num} WHERE spool_id='0' and cameras_per_pull={max_spools} and camera_id={row[3]} and camera_length={row[4]} and barn_id={barn_ids} and pull_id={pull_ids}")
    c.execute(max_pull)
    records = c.fetchall()
    for row in records:
        if row[5] != 0:
            update_bp_db(check_db, row[0], row[1], row[3])
    biggest_barn = f"SELECT * FROM {db_name} WHERE barn_id={barn_ids}"
    c.execute(biggest_barn)
    records = c.fetchall()
    num = 0
    for row in records:
        b_check = row[0] 
        p_check = row[1]
        cp_check = row[2]
        c_check = row[3]
        l_check = row[4]
        bp_check = bp_db_check(check_db, b_check, p_check, c_check)
        if 0 == bp_check:
            num += 1
            spool_qty = len(dict_name)
            if num <= spool_qty:
                spool_amounts = get_spool_amount(num, dict_name)
                if spool_amounts >= l_check:
                    update_spool_dict(num, l_check, dict_name)
                    c.execute(f"UPDATE {db_name} SET spool_id={num} WHERE barn_id={b_check} and pull_id={p_check} and cameras_per_pull={cp_check} and camera_id={c_check} and camera_length={l_check} and spool_id='0'")
                    update_bp_db(check_db, b_check, p_check, c_check)
            else:
                if p_check > 2:
                    if spool_amounts >= l_check:
                        update_spool_dict(1, l_check, dict_name)
                        c.execute(f"UPDATE {db_name} SET spool_id={1} WHERE barn_id={b_check} and pull_id={p_check} and cameras_per_pull={cp_check} and camera_id={c_check} and camera_length={l_check} and spool_id='0'")
                        update_bp_db(check_db, b_check, p_check, c_check)
    b_list.remove(barn_ids)       

def main_function():
    
    cat_six = "catsix"
    g_c = "gc"
    
    cspb = "bp"
    gcbp = "gc_bp"
    
    row_count_catsix = "SELECT COUNT(*) FROM catsix"
    row_count_gc = "SELECT COUNT(*) FROM gc"
    
    all_catsix = "SELECT * FROM catsix"
    all_gc = "SELECT * FROM gc"
    
    catsix_display_name = "CAT6 SPOOLS"
    gc_display_name = "GAME CHANGER SPOOLS"

    print("*********************\n")
    print("   TOTAL RESULTS")
    print("\n*********************\n\n\n")

    check_db = compare_database(row_count_catsix)
    if check_db >= 1:
        total_catsix_length = "SELECT SUM(camera_length) FROM catsix"
        spool_creator(total_catsix_length, catsix_spool_check)
        max_spool_qty = max(cam_pull_list)
        num_spools = len(catsix_spool_check)
        max_camera_pull(num_spools, max_spool_qty, catsix_spool_check)
        max_catsix_pull = f"SELECT * FROM catsix WHERE cameras_per_pull={max_spool_qty} LIMIT {max_spool_qty}"
        biggest_pull(max_catsix_pull, barn_list, cspb, cat_six, max_spool_qty, catsix_spool_check)
        for b in barn_list:
            next_barn = f"SELECT * FROM {cat_six} WHERE barn_id={b}"
            finish_barns(next_barn, cat_six, cspb, catsix_spool_check)
        nums_spools = len(catsix_spool_check)
        print(f"    {catsix_display_name}")
        print("   -------------\n")
        print(f"TOTAL SPOOLS NEEDED: {nums_spools}\n\n")
        for s in catsix_spool_check.keys():
            spool_select = f"SELECT * FROM {cat_six} WHERE spool_id={s}"
            display_results(spool_select, s)
            results_l = get_spool_amount(s, catsix_spool_check)
            print(f"                  | Waste:  {results_l} ft\n\n")
            
    check_db = compare_database(row_count_gc)
    if check_db >= 1:
        total_gc_length = "SELECT SUM(camera_length) FROM gc"
        spool_creator(total_gc_length, gc_spool_check)
        gc_max_spool_qty = max(gc_cam_pull_list)
        gc_num_spools = len(gc_spool_check) 
        max_camera_pull(gc_num_spools, gc_max_spool_qty, gc_spool_check)
        max_gc_pull = f"SELECT * FROM gc WHERE cameras_per_pull={gc_max_spool_qty} LIMIT {gc_max_spool_qty}"
        biggest_pull(max_gc_pull, gc_barn_list, gcbp, g_c, gc_max_spool_qty, gc_spool_check)
        for b in gc_barn_list:
            next_barn = f"SELECT * FROM {g_c} WHERE barn_id={b}"
            finish_barns(next_barn, g_c, gcbp, gc_spool_check)
        gcnums_spools = len(gc_spool_check)
        print(f"    {gc_display_name}")
        print("   ---------------------\n")
        print(f"TOTAL SPOOLS NEEDED: {gcnums_spools}\n\n")            # this does not show true amount, it counts spools not used
        for s, l in gc_spool_check.items():
            if l != 0:                                                     # this line is a catchall until I fix the issue. it makes sure it doesn't display spools that have not been used
                spool_select = f"SELECT * FROM {g_c} WHERE spool_id={s}"
                display_results(spool_select, s)
                results_l = get_spool_amount(s, gc_spool_check)
                print(f"                  | Waste:  {results_l} ft\n\n")
                
    exit()

def compare_pulls(db1, db2):
    c.execute(db1)
    c2.execute(db2)
    records = c.fetchall()
    records_two = c2.fetchall()
    for db1row, db2row in zip_longest(records, records_two):
        try:
            if db1row[0] == db2row[0]:
                if db1row[1] == db2row[1]:
                    if db1row[2] == db2row[2]:    
                        row_count_catsix = f"SELECT COUNT(*) FROM catsix WHERE barn_id={db1row[0]} and pull_id={db1row[1]} and cameras_per_pull={db1row[2]}"
                        row_count_gc = f"SELECT COUNT(*) FROM gc WHERE barn_id={db2row[0]} and pull_id={db2row[1]} and cameras_per_pull={db2row[2]}"
                        catsix_row = compare_database(row_count_catsix)
                        cam_pull_list.remove(db1row[2])
                        cam_pull_list.append(catsix_row)
                        gc_row = compare_database(row_count_gc)
                        gc_cam_pull_list.remove(db2row[2])
                        gc_cam_pull_list.append(gc_row)
                        c.execute(f"UPDATE catsix SET cameras_per_pull={catsix_row} WHERE barn_id={db1row[0]} and pull_id={db1row[1]} and cameras_per_pull={db1row[2]}")
                        c.execute(f"UPDATE gc SET cameras_per_pull={gc_row} WHERE barn_id={db2row[0]} and pull_id={db2row[1]} and cameras_per_pull={db2row[2]}")
        except:
            pass
        
def barn_no_dup_list(cable_db, lists):
    c.execute(cable_db)
    records = c.fetchall()
    set_list = []
    for row in records:
        set_list.append(row[0])
    no_dup_set = set(set_list)
    new_lists = list(no_dup_set)
    lists.extend(new_lists)
    
def cameras_per_pull_no_dup(lists):
    no_dup_set = set(lists)
    lists.clear()
    new_lists = list(no_dup_set)
    lists.extend(new_lists)
                    
def add_farm_info():
    print(" ")
    clear()
    print("Enter each barn one at a time")
    print("")
    while True:
        try:
            barn_n = (int(input("Enter Barn Number: ")))
            if barn_n not in b_check_list:
                b_check_list.append(barn_n)
            else:
                print("\nBarn number already entered. Please enter another barn number")
                time.sleep(3)
                add_farm_info()
            print("")
            cable_pull_num = (int(input(f"How many cable pulls for Barn {barn_n}?: ")))
            
            for i in range(1, cable_pull_num+1):
                pull_num = i
                print("")
                cameras_per_pull = (int(input(f"How many cameras for Barn {barn_n}, Pull {pull_num}?: ")))
                
                for i in range(1, cameras_per_pull+1):
                    
                    print("")
                    if i == 1:
                        camera_id = (int(input(f"Enter Camera number for Barn {barn_n}, Pull {pull_num}: ")))
                    else:
                        camera_id = (int(input(f"Enter Next Camera number for Barn {barn_n}, Pull {pull_num}: ")))
                        
                    camera_length = int(input(f"Enter Length for Camera {camera_id}: "))
                    
                    
                    
                    if camera_length <= 328:
                        c.execute("INSERT INTO catsix VALUES (:barn_id, :pull_id, :cameras_per_pull, :camera_id, :camera_length, :spool_id)", (barn_n, pull_num, cameras_per_pull, camera_id, camera_length, 0))
                        get_pulls(barn_n, pull_num, camera_id)
                        cam_pull_list.append(cameras_per_pull)
                        
                    
                    else:
                        c.execute("INSERT INTO gc VALUES (:barn_id, :pull_id, :cameras_per_pull, :camera_id, :camera_length, :spool_id)", (barn_n, pull_num, cameras_per_pull, camera_id, camera_length, 0))
                        get_gc_pulls(barn_n, pull_num, camera_id)
                        gc_cam_pull_list.append(cameras_per_pull)
                        
                    
                    
        except ValueError:
            print("\nPlease enter a number, starting all over")
            time.sleep(3)
            add_farm_info()
                    
        while True:
            print("")
            cont = input("Do you want to add another barn? y/n ")
            if cont == "y":
                add_farm_info()
            elif cont == "n":
                clear()
                cameras_per_pull_no_dup(cam_pull_list)
                cameras_per_pull_no_dup(gc_cam_pull_list)
                barn_no_dup_list(all_catsix, barn_list)
                barn_no_dup_list(all_gc, gc_barn_list)
                print(cam_pull_list)
                print(gc_cam_pull_list)
                print(" ")
                compare_pulls(all_catsix, all_gc)
                print(cam_pull_list)
                print(gc_cam_pull_list)
                main_function()
            else:
                print("Please enter Y or N")

def intro():
    clear()
    print("Welcome to SPOOL TOOL!\n")
    input("Press Any Key to Continue...")
    print(" ")
    clear()
    add_farm_info()


if __name__ == '__main__':
    intro()


