#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
from glob import glob
import sys
from datetime import datetime, timedelta
from collections import OrderedDict
import time
import settings
import queries
from utils import *


# --------------------------------------------------------------------------------
# список запитів, які повинні виконатись, щоб відправити на сервер змінені
# (з моменту попереднього запуску скрипту) дані довідників та чеки:
# --------------------------------------------------------------------------------

q = {}
fn = {}

## список одиниць виміру:
fn['unit'] = '1-unit.csv'
q['unit'] = queries.unit_query

# перелік магазинів і терміналів (кас):
fn['terminal'] = '2-terminal.csv'
q['terminal'] =  queries.terminal_query

# перелік магазинів і їх назви:
#fn['shop'] = '3-shop.csv'
#q['shop'] = queries.shop_query

# список касирів (код, ПІП):
fn['cashier'] = '4-cashier.csv'
q['cashier'] = queries.cashier_query

# дерево категорій:
fn['category'] = '5-category.csv'
q['category'] = queries.category_query

# список товарів:
fn['product'] = '6-product.csv'
q['product'] = queries.product_query

# чеки
fn['receipt'] = '7-receipt-%i.csv'
q['receipt'] = queries.receipt_query


#-----------------------------------------------------------------------------
# 2. завантажуємо дельту по одиницям виміру (unit): //////////////////////////
#-----------------------------------------------------------------------------
def load_units(cursor):
    log('ОБРОБКА `Unit`')
    print (updatenum['pack'][PREV], updatenum['pack'][CUR])
    query = q['unit'] % (
        updatenum['pack'][PREV], updatenum['pack'][CUR]
    )
    #print query
    ID=0
    NAME=1
    PACKED=2
    PACK_CAPACITY=3
    rows = load_sql(cursor, query)
    if rows:
        t1 = time.time()
        filename = iter_dir_name + '/' + fn['unit']
        print filename
        write_csv(rows, filename )
        t2 = time.time()
        log('Файл %s збережено за %s sec. Записів: %i'
            % (filename, str(timedelta(seconds=t2-t1)), len(rows))
        )

        ii=0
        t1 = time.time()
        data_list = []
        for row in rows:
            ii += 1
            data = {
                'unit_id': row[ID],
                'name': row[NAME],
                'packed': row[PACKED],
                'pack_capacity': row[PACK_CAPACITY]
            }
            data_list.append(data)

        retcode, result = POST(
            'http://bi.datawiz.io/api/v1/units/',
            data=data_list,
            key_id=KEY_ID,
            secret=SECRET
        )
        if retcode == ERROR:
            errlog(
                u'Помилка передачі даних через API для `Unit`'
            )
            errlog(
                'При наступному запуску ітерація #%i буде повторена'
                % iteration_number
            )
            errlog('Програма припинена з кодом помилки 1')
            exit_and_send_error_message()
        t2 = time.time()
        log('На сервер bi.datawiz.io/api за %s sec передано %i записів по '
            'довіднику `Unit` (~%s зап/сек)' %
            (str(timedelta(seconds=t2-t1)), ii, round(ii/(t2-t1),2))
        )
    else:
        log('Довідник `unit` не змінювався. Нічого зберігати не потрібно.')


#-----------------------------------------------------------------------------
# 3. завантажуємо дельту по касирам (cashier): ///////////////////////////////
#-----------------------------------------------------------------------------
def load_cashiers(cursor):
    log('ОБРОБКА `Cashier`')
    print (updatenum['cashier'][PREV], updatenum['cashier'][CUR])
    query = q['cashier'] % (
        updatenum['cashier'][PREV], updatenum['cashier'][CUR]
    )
    #print query
    rows = load_sql(cursor, query)
    if rows:
        t1 = time.time()
        filename = iter_dir_name + '/' + fn['cashier']
        print filename
        write_csv(rows, filename )
        t2 = time.time()
        log('Файл %s збережено за %s sec. Записів: %i'
            % (filename, str(timedelta(seconds=t2-t1)), len(rows))
        )

        ii=0
        t1 = time.time()
        data_list = []
        for row in rows:
            ii += 1
            data = { 'cashier_id': row[0], 'name': row[1] }
            data_list.append(data)

        retcode, result = POST(
            'http://bi.datawiz.io/api/v1/cashiers/',
            data=data_list,
            key_id=KEY_ID,
            secret=SECRET
        )
        if retcode == ERROR:
            errlog(
                u'Помилка передачі даних через API для `Cashier` '
                #'{id=%i, name=%s}' % (row[0], row[1])
            )
            errlog(
                'При наступному запуску ітерація #%i буде повторена'
                % iteration_number
            )
            errlog('Програма припинена з кодом помилки 1')
            exit_and_send_error_message()
        t2 = time.time()
        log('На сервер bi.datawiz.io/api за %s sec передано %i записів по '
            'довіднику `Cashier` (~%s зап/сек)' %
            (str(timedelta(seconds=t2-t1)), ii, round(ii/(t2-t1),2))
        )
    else:
        log('Довідник `Cashier` не змінювався. Нічого зберігати не потрібно.')


def _order_categories(cat, cdict, ordict):
    cid = cat['category_id']
    parent_id = cat['parent_id']
    if parent_id in cdict:
        _order_categories(cdict[parent_id], cdict, ordict)
    if cid not in ordict:
        ordict[cid] = cat

#-----------------------------------------------------------------------------
# 4. завантажуємо дельту по категоріям (categories): /////////////////////////
#-----------------------------------------------------------------------------
def load_category(cursor):
    log('ОБРОБКА Category')
    ID=0
    PARENTID=1
    NAME=2
    query = q['category'] % (
        updatenum['category'][PREV],
        updatenum['category'][CUR]
    )
    rows = load_sql(cursor, query)
    if rows:
        ii=0
        cat_dict = {}
        orddict = OrderedDict()
        for row in rows:
            ii += 1
            cid = row[ID]
            parent_id = row[PARENTID]
            name = row[NAME]
            cat = { 'category_id': cid, 'parent_id': parent_id, 'name': name }
            cat_dict[cid] = cat
        for cat in cat_dict.values():
            _order_categories(cat, cat_dict, orddict)
        cat_list = orddict.values()

        t1 = time.time()
        filename = iter_dir_name + '/' + fn['category']
        write_csv(rows, filename)
        t2 = time.time()
        log('Файл %s збережено за %s sec. Записів: %i' %
            (filename, str(timedelta(seconds=t2-t1)), len(rows))
        )

        t1 = time.time()
        #for cat in cat_list:
        #    #log(cat)
        retcode, result = POST(
            'http://bi.datawiz.io/api/v1/categories/',
            data=cat_list,
            key_id=KEY_ID,
            secret=SECRET
        )
        if retcode == ERROR:
            errlog(
                u'Помилка передачі даних через API для category '
                #'{id=%(category_id)i, parent_id=%(parent_id)s, name=%(name)s}' % cat
            )
            errlog(
                'При наступному запуску ітерація #%i буде повторена'
                % iteration_number
            )
            errlog('Програма припинена з кодом помилки 1')
            exit_and_send_error_message()
        t2 = time.time()
        log('Через API на сервер bi.datawiz.io/api за %s sec було передано %i записів по '
            'довіднику Category (~%s зап/сек)' %
            (str(timedelta(seconds=t2-t1)), ii, round(ii/(t2-t1),2))
        )
    else:
        log('Довідник `%s` не змінювався. Нічого зберігати не потрібно.' % 'category')


##-----------------------------------------------------------------------------
## 5. завантажуємо дельту по продуктам (products): ////////////////////////////
##-----------------------------------------------------------------------------
def load_product(cursor):
    log('ОБРОБКА Product')
    ID=0
    CATEGORYID=1
    UNITID=2
    NAME=3
    query = q['product'] % (
        updatenum['product'][PREV],
        updatenum['product'][CUR],
        updatenum['pack'][PREV],
        updatenum['pack'][CUR],
    )
    rows = load_sql(cursor, query)
    if rows:
        t1 = time.time()
        filename = iter_dir_name + '/' + fn['product']
        write_csv(rows, filename)
        t2 = time.time()
        log('Файл %s збережено за %s sec. Записів: %i' %
            (filename, str(timedelta(seconds=t2-t1)), len(rows))
        )

        t1 = time.time()
        ii=0
        prod_list = []
        for row in rows:
            ii += 1
            pid = row[ID]
            category_id = row[CATEGORYID]
            unit_id = row[UNITID]
            name = row[NAME]
            prod = {
                'product_id': pid,
                'category_id': category_id,
                'unit_id': unit_id,
                'name': name
            }
            prod_list.append(prod)

        retcode, result = POST(
            'http://bi.datawiz.io/api/v1/products/',
            data=prod_list,
            key_id=KEY_ID,
            secret=SECRET
        )
        if retcode == ERROR:
            #prod['ii'] = ii
            errlog(
                u'Помилка передачі даних через API для product'  #%(ii)i '
                #'{id=%(product_id)s, category_id=%(category_id)s, '
                #'unit_id=%(unit_id)s, name=%(name)s}' % prod
            )
            errlog(
                'При наступному запуску ітерація #%i буде повторена'
                % iteration_number
            )
            errlog('Програма припинена з кодом помилки 1')
            exit_and_send_error_message()
        t2 = time.time()
        log('На сервер bi.datawiz.io/api за %s sec було передано %i записів по '
            'довіднику Product (~%f зап/сек)' %
            (str(timedelta(seconds=t2-t1)), ii, round(ii/(t2-t1),2))
        )
    else:
        log('Довідник `Product` не змінювався. Нічого зберігати не потрібно.')


##-----------------------------------------------------------------------------
## 6. завантажуємо дельту по чекам (receipts): ////////////////////////////////
##-----------------------------------------------------------------------------
def load_receipts(cursor):
    #import pudb; pudb.set_trace()
    # sales_dt, terminal_identifier, receipt_identifier,
    # posnum, price, total_price, qty, product_identifier, packed,
    # cashier_identifier
    log('ОБРОБКА Receipts')

    DATE = 0
    TERM_ID = 1
    RECEIPT_ID = 2
    POSNUM = 3
    PRICE = 4
    TOTAL_PRICE = 5
    QTY = 6
    PRODUCT_ID = 7
    PACKED = 8
    CASHIER_ID = 9

    BLOCK = 1000 # скільки об'єктів виванажувати за один раз

    # підвантажуємо кожний магазин окремо:
    for shop_id, lastdates in sales_lastdate.items():
        print 'shop_id=', shop_id, 'sales_lastdate[shop_id][PREV]=',\
            sales_lastdate[shop_id][PREV], 'sales_lastdate[shop_id][CUR]=',\
            sales_lastdate[shop_id][CUR]
        query = q['receipt'] % {
            'shop_id':   shop_id,
            'date_from': sales_lastdate[shop_id][PREV],
            'date_to':   sales_lastdate[shop_id][CUR]
        }
        ii=0 # к-сть (підрахунок) cartitems
        iii=0 # к-сть (підрахунок) receipts
        beg_iii=0 # номер першого receipt в блоці (при зававантаженні блоками)
        receipt = None
        rows = load_sql(cursor, query)
        if rows:
            t1 = time.time()
            filename = iter_dir_name + '/' + fn['receipt'] % shop_id
            write_csv(rows, filename)
            t2 = time.time()
            log('Файл %s збережено за %s sec. Записів: %i' %
                (filename, str(timedelta(seconds=t2-t1)), len(rows))
            )
            t1=time.time()
            receipt_list = []
            for row in rows:
                ii += 1

                #shop_identifier = shop_id #row[SHOP_ID]
                term_identifier = row[TERM_ID]
                order_id = row[RECEIPT_ID]
                posnum = row[POSNUM]
                cashier_identifier = row[CASHIER_ID]
                dt = datetime.strptime(row[DATE], '%Y%m%d%H%M%S')\
                    .strftime('%Y-%m-%dT%H:%M:%S')
                pid = row[PRODUCT_ID]
                price = float(row[PRICE])
                base_price = price
                qty = float(row[QTY])
                total_price = float(row[TOTAL_PRICE])

                if (not receipt or (
                        receipt and (
                            receipt['terminal_id'] != term_identifier or
                            receipt['order_id'] != order_id or
                            receipt['date'] != dt
                        )
                    )
                ):
                    if receipt:
                        #log( 'save receipt: %s_%s_%s' % ( term_identifier, dt, order_id ))
                        receipt_list.append(receipt)

                        if len(receipt_list) >= BLOCK:
                            retcode, new_product = POST(
                                'http://bi.datawiz.io/api/v1/receipts/',
                                data=receipt_list,
                                key_id=KEY_ID,
                                secret=SECRET
                            )
                            if retcode == ERROR:
                                #receipt['ii'] = ii
                                errlog(
                                    u'Помилка передачі даних через API для receipt' #%(ii)i '
                                    #'{date=%(date)s, order_id=%(order_id)s, '
                                    #'terminal_id=%(terminal_id)s, cashier_id=%(cashier_id)s}' % receipt
                                )
                                errlog(
                                    'При наступному запуску ітерація #%i буде повторена'
                                    % iteration_number
                                )
                                errlog('Програма припинена з кодом помилки 1')
                                exit_and_send_error_message()

                            # порцію даних передал - список обнулили:
                            log(u'Передано чеків %i - %i' % (beg_iii, iii))
                            beg_iii = iii+1
                            receipt_list = []
                    iii += 1
                    receipt = {}
                    receipt['date'] = dt
                    receipt['order_id'] = order_id
                    receipt['terminal_id'] = term_identifier
                    receipt['cashier_id'] = cashier_identifier
                    receipt['cartitems'] = []

                if qty == 0:
                   log( "QTY == 0 !" )
                   log( '><>>>>', row )
                   log( 'ROW #', ii )
                   continue

                if total_price == 0:
                   log( "TOTAL_PRICE == 0 !" )
                   log( '><>>>>', row )
                   log( 'ROW #', ii )
                   continue

                if round(price,2) <> round(qty*total_price,2):
                    price = round( total_price / qty, 2)

                cartitem = {}
                cartitem['product_id'] = pid
                cartitem['order_no'] = posnum
                cartitem['base_price'] = base_price
                cartitem['price'] = price
                cartitem['qty'] = qty
                cartitem['total_price']= total_price
                receipt['cartitems'].append(cartitem)

            #log( 'save receipt: %s_%s_%s' % ( term_identifier, dt, order_id ))
            receipt_list.append(receipt)

            retcode, new_product = POST(
                'http://bi.datawiz.io/api/v1/receipts/',
                data=receipt_list,
                key_id=KEY_ID,
                secret=SECRET
            )
            if retcode == ERROR:
                #prod['ii'] = ii
                errlog(
                    u'Помилка передачі даних через API для receipt' #%(ii)i '
                    #'{date=%(date)s, order_id=%(order_id)s, '
                    #'terminal_id=%(terminal_id)s, cashier_id=%(cashier_id)s}' % receipt
                )
                errlog(
                    'При наступному запуску ітерація #%i буде повторена'
                    % iteration_number
                )
                errlog('Програма припинена з кодом помилки 1')
                exit_and_send_error_message()
            # порцію даних передал - список обнулили:
            log(u'Передано чеків %i - %i' % (beg_iii, iii))

            t2=time.time()
            log('На сервер bi.datawiz.io за %s sec було '
                'передано %i позицій(ю) по магазину %s (~%s позицій/сек)'
                % (str(timedelta(seconds=t2-t1)), ii, shop_id, round(ii/(t2-t1),2) )
            )
        else:
            log('Нових чеків по магазину %s нема. Пропускаємо...' % shop_id)




# ----------------------------------------------------------------------------------
# BEGIN
# ----------------------------------------------------------------------------------
if __name__ == '__main__':
    init_log()

    #------------------------------------------------------------------------------
    # CONNECTION STRING:
    #------------------------------------------------------------------------------
    dsn = settings.DSN
    db_user = settings.DB_USER
    db_password = settings.DB_PASSWORD
    database = settings.DATABASE
    con_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;' % (
        dsn, db_user, db_password, database
    )

    try:
        cnxn = pyodbc.connect(con_string)
        cursor = cnxn.cursor()
    except Exception as e:
        errlog('Exception: %s' % e)
        exit_and_send_error_message()
    #-------------------------------------------------------------------------------

    # API_KEY користувача (має відповідати API_KEY з api1_authinfo)
    KEY_ID = settings.KEY_ID
    # секретний рядок користувача (має відповідати SECRET з api1_authinfo)
    SECRET = settings.SECRET

    # назва файлу, в якому зберігається номер останного циклу передачі даних між
    # серверами (клієнта і Datawiz) автоматично збільшується на 1):
    iteration_number_filename = 'iteration_number.txt'

    # номер циклу передачі даних між серверами (iteration_number) використовується
    # для логування процесу передачі даних

    # Принцип логування:
    # в кожній ітерації створюється каталог з назвою рівною iteration_number
    # в цей каталог вносятья дельта файли (у форматі csv), які передаються на
    # сервер. Також в цьому каталозі зберігаються файли `datepump_filename`
    # `sales-lastdate` на основі яких вивантажувались дельта-файли
    # (опис цих файлів див.нижче)
    iter_dir = '%06i'

    # updatenum всіх довідників, щоб вивантажити тільки зміни з попереднього циклу:
    datapump_filename = 'datapump.csv'
    datapump_query = queries.datapump_query

    # зчитуємо дати останніх чеків в базі даних по кожному з магазинів.
    sales_lastdate_filename = 'sales-lastdate.csv'
    sales_lastdate_query = queries.sales_lastdate_query

    # Перевіряємо, чи цей скрипт вже не працює (на файловій системі в поточному
    # каталозі створюється файл 'RUNNING'. Якщо такий файл вже існує - значить
    # скрипт вже працює - вважаємо це коллізією і повідомляємо про помилку
    # + відправляємо листа. Якщо ж такого файлу НЕ існує, то створюємо його і
    # продовжуємо роботу
    if os.path.isfile(RUN_LABEL):
        errlog(
            'При спробі запуску скрипта було виявлено, що попередній '
            'примірник скрипта ще не закінчив свою роботу'
        )
        #еrrlog(u"(виявлено файл-мітку 'RUNNING', створений попередньою ітерацією).")
        errlog("Чекаємо завершення роботи попереднього примірника скрипта.")
        errlog(
            "Якщо скрипт довго не завершується - перевірте лог файл і при "
            "потребі вилучте файл-мітку 'RUNNING' з поточного каталога скрипта"
        )
        errlog('При наступній ітерації спроба запуску скрипта буде повторена')
        errlog('Програма припинена з кодом помилки 1')
        exit_and_send_error_message(keep_running=True)

    # створюємо свою мітку 'RUNNING' i продовжуємо роботу:
    touch(RUN_LABEL)

    ##-----------------------------------------------------------------------------
    ## завантажуємо дельту по чекам (receipts): ///////////////////////////////////
    ##-----------------------------------------------------------------------------
    # 1. завантажуємо останні дати змін чеків по магазинам:
    log('Load the last sales dates per shop...')
    cur_sales_lastdate_rows = load_sql(cursor, sales_lastdate_query)

    # чекаємо 30 сек, щоб були довантажені всі дані до дати, яку ми виставили, як
    # кінцеву
    log('Wait 30 sec...')
    time.sleep(30)

    # 2. завантажуємо попередні останні дати змін чеків по магазинамв (якщо вони є):
    log('Load previous sales dates per shop...')
    if os.path.isfile(sales_lastdate_filename):
        prev_sales_lastdate_rows = [
            [ int(i[0]), int(i[1]) ] for i in read_csv(sales_lastdate_filename) if i
        ]
    else:
        prev_sales_lastdate_rows = []

    print '='*100
    print "prev_sales_lastdate_rows=", prev_sales_lastdate_rows
    print '-'*100
    print "cur_sales_lastdate_rows=", cur_sales_lastdate_rows
    print '='*100

    # приєднуємо prev sales_lastdate до cur cur_sales_lastdate і результат зберігаємо
    # в довіднику у вигляді:
    #   sales_lastdate['shopid'] = (prev_sales_lastdate, cur_sales_lastdate)
    PREV=0
    CUR=1
    KEY=0
    DT=1
    sales_lastdate = dict(
        [ (row[CUR][KEY], (int(row[PREV][DT]), int(row[CUR][DT])))
            for row in full_outer_join(
                prev_sales_lastdate_rows,
                cur_sales_lastdate_rows,
                0,
                (0, 0)
            )
        ]
    )
    print '>>>>>> Sales_lastdate:', sales_lastdate
    print '='*100

    # читаємо номер циклу доступу до бази даних
    if os.path.isfile(iteration_number_filename):
        try:
            with open(iteration_number_filename, 'r') as f:
                line = f.readline()
                if line:
                    try:
                        iteration_number = int(line)
                    except:
                        iteration_number = 1
        except IOError, e:
            errlog(e)
            errlog('Програма припинена з кодом помилки 1')
            exit_and_send_error_message()
    else:
        iteration_number = 1

    # пробуємо записати iteration_number назад у файл (перевірка прав доступу):
    try:
        with open(iteration_number_filename, 'w') as f:
            print >>f, iteration_number
    except IOError, e:
        errlog(e)
        errlog('Не можу писати у файл `%s`. Перевірте вільний простір на диску або '
               'права доступу до файлу' % iteration_number_filename
              )
        errlog('Програма припинена з кодом помилки 1')
        exit_and_send_error_message()

    iter_dir_name = iter_dir % iteration_number

    if not glob(iter_dir_name):
        try:
            os.mkdir(iter_dir_name)
        except IOError, e:
            errlog(e)
            errlog(
                'Не можу створити каталог `%s`. Перевірте права доступу'
                % iter_dir_name
            )

    log(em('-- BEGIN ITERATION #%s ' %iteration_number +'-'*60))
    subject = 'ERROR message (ITERATION #%s) from Kolos Datawiz API'\
        %iteration_number

    # 1. завантажуємо останні updatenum для довідників:
    cur_updatenum_rows = load_sql(cursor, datapump_query)

    # 2. завантажуємо попередні updatenum для довідників (якщо вони є):
    if os.path.isfile(datapump_filename):
        prev_updatenum_rows = read_csv(datapump_filename)
    else:
        prev_updatenum_rows = []

    print '='*100
    print 'prev_updatenum_rows=', prev_updatenum_rows
    print '-'*100
    print 'cur_updatenum_rows=', cur_updatenum_rows
    print '='*100

    # приєднуємо prev updatenum до cur updatenum і результат зберігаємо
    # в довіднику у вигляді:
    #   updatenum['dictname'] = (prev_updatenum, cur_updatenum)
    PREV=0
    CUR=1
    KEY=0
    UPDATENUM=1
    updatenum = dict(
        [ (row[CUR][KEY], (int(row[PREV][UPDATENUM]), int(row[CUR][UPDATENUM])))
            for row in full_outer_join(
                prev_updatenum_rows,
                cur_updatenum_rows,
                0,
                (0, 0)
            )
        ]
    )
    print 'UPDATENUM:', updatenum

    # завантажуємо дельту по довідникам:
    load_units(cursor)
    load_cashiers(cursor)
    load_category(cursor)
    load_product(cursor)

    ##-----------------------------------------------------------------------------
    ## довідники збережені. зберігаємо cur updatenum, як prev updatenum: /////////
    ##-----------------------------------------------------------------------------
    if os.path.isfile(datapump_filename):
        os.rename(datapump_filename, iter_dir_name+'/'+datapump_filename)
    write_csv(cur_updatenum_rows, datapump_filename)

    # завантажуємо дельту по чекам:
    load_receipts(cursor)

    ##-----------------------------------------------------------------------------
    ## чеки збережені. зберігаємо cur sales_lastdate, як prev sales_lastdate //////
    ##-----------------------------------------------------------------------------
    if os.path.isfile(sales_lastdate_filename):
        # копіюємо попередній файл в папку ітерації
        os.rename(sales_lastdate_filename, iter_dir_name+'/'+sales_lastdate_filename)
    write_csv(cur_sales_lastdate_rows, sales_lastdate_filename)

    # зберігаємо НАСТУПНИЙ itration_number
    with open(iteration_number_filename, 'w') as f:
        print >>f, iteration_number + 1

    t2=time.time()
    log('-- ITERATION #%s DONE SUCCESSFULLY! (скрипт працював %s sec) '
        % (iteration_number, str(timedelta(seconds=t2-t0)))
        +'-'*30
    )

    # якщо попередня ітерація завершилась помилкою,
    # а ця - успішно, то відправляємо листа про успішність завершення ітерації:
    if get_last_retcode() != 0:
        subject = 'SUCCESSFUL message (ITERATION #%s) from Kolos Datawiz API' \
            % iteration_number

        msg_list = [
            'Cкрипт %s, запущенний в %s, відпрацював успішно!' % (
                FULL_SCRIPT_NAME,
                str(datetime.fromtimestamp(int(t0)))
            ),
            ''
            'Скрипт працював %s sec' % str(timedelta(seconds=t2-t0))
        ]

        message = '\n'.join(msg_list)
        try:
            send_mail(message, subject)
        except Exception as e:
            errlog('Невдача при відправці ел.пошти:')
            errlog('Exception: %s' %e)
            errlog('Ігноруємо відправку. :-(')

    # зберігаємо у файлі останній код повернення - 0 :
    retcode = 0
    save_last_retcode(retcode)

    # Програма успішно завершується - вилучаємо файл-ознаку "RUNNING":
    if os.path.isfile(RUN_LABEL):
       os.remove(RUN_LABEL)

    # повертаємо код завершення - 0
    sys.exit(retcode)

