# coding: utf-8

import os
import sys
import codecs
import cStringIO
import csv
import settings
import pyodbc
import smtplib
from datetime import datetime, timedelta
import time
from inspect import currentframe, getframeinfo
import requests
import json
from email.mime.text import MIMEText
from httpsig.requests_auth import HTTPSignatureAuth
import time


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# GLOBAL VARIABLES
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
RUN_LABEL = 'RUNNING' # назва файлу-мітки, по якому визначаємо,
                      # що скрипт вже запущенно

t0 = time.time()

# -------------------------------------------------------------------------
# LOGGER
# -------------------------------------------------------------------------
LOG_LEVEL = 'INFO'
LOG_TO_CON = True # дублювати запис в лог друком в консоль

# -------------------------------------------------------------------------
# коди повернення з функції POST:
# -------------------------------------------------------------------------
ERROR = 0
OK = 1

# повна назва скрипта (з повним шляхом):
FULL_SCRIPT_NAME = os.path.abspath(sys.argv[0])

# -------------------------------------------------------------------------
# last_retcode_filename - назва файлу, в якому зберігаємо останній код
# завершення скрипта. Якщо останній код був 1 (помилка), а зараз
# скрипт завершується успішно (0), відправляємо один "успішний" лист
# адміністратору(ам) після одного (або декількох) листів з помилками.
# Це дозволяє зрозуміти, що помилка була "автоматично" виправлена
# після серії ітерацій.
# -------------------------------------------------------------------------
last_retcode_filename = 'last_retcode.txt'


# -------------------------------------------------------------------------
# тут зберігається subject листа про помилку:
# -------------------------------------------------------------------------
subject = settings.SUBJECT


# -------------------------------------------------------------------------
# в цьому списку накоплюємо повідомлення про помилку, які відправлятимемо
# електронною поштою:
# -------------------------------------------------------------------------
error_log_list = [
    'Cкрипт %s, запущенний в %s, завершився з помилкою:' % (
        FULL_SCRIPT_NAME,
        str(datetime.fromtimestamp(int(t0)))
    ),
    ''
]

#------------------------------------------------------------------------------
# назва глобального лог-файлу (в нього пишуться аварійні команди, якщо взагалі
# почати роботу не вдається)
#------------------------------------------------------------------------------
log_file_name = 'log.txt'

#------------------------------------------------------------------------------
# глобальна змінна типу "Файл". В якій зберігається вказівник на лог-файл, куди
# пишеться лог
#------------------------------------------------------------------------------
log_f = None



#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# ФУНКЦІЇ ТА ПРОЦЕДУРИ
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

#........................................................................
def nvl(ifnull, defval):
    """ -----------------------------------------------------------------
    return original value if <ifnull> parameter is not None
    replace None value with defval value otherwise
    ----------------------------------------------------------------- """
    return defval if ifnull is None else ifnull


#........................................................................
def full_outer_join(left_list, right_list, join_field_index, dummy_tuple):
    """ ------------------------------------------------------------------
    з'єднує докупи два списка по полю-ключу по принципу "FULL OUTER JOIN"
    що означає:
     а. якщо є дані в лівому списку, а в правому нема, то
        - беремо лівий список, правий - порожній
     б. якщо є дані в правому списку, а в лівому нема, то
        - беремо правий список, лівий - порожній
     в. якщо є дані і в лівому списку і в правому списку, то
        - беремо і лівий список і правий список
     ЗАУВАЖЕННЯ: під "порожні дані" вважаємо дані, передані,
                 як dummy_tuple

    # full outer join left_list with right_list by join_field
    # structure of left_list and right_list must look like
    # [ [ field1, field2, field3, ...], [ field1, field2, ...
    # or [ ( field1, field2, field3, ...], [ field1, field2, ...
    # or ( ( field1, field2, field3, ...], [ field1, field2, ...
    # dummy_tuple is used to replace abcence data from left or right lists
     ------------------------------------------------------------------ """
    assert isinstance(left_list, (list, tuple))
    assert isinstance(right_list, (list, tuple))
    assert isinstance(join_field_index, int)
    assert isinstance(dummy_tuple, (list, tuple))

    result_list = []
    il=-len(left_list); ir=-len(right_list)
    while il or ir:
        if il: l=left_list[il]
        if ir: r=right_list[ir]

        if il and ir:
            if l[join_field_index] == r[join_field_index]:
                result_list.append((l,r))
                ir+=1
                il+=1
            elif l[join_field_index] < r[join_field_index]:
                result_list.append((l,dummy_tuple))
                il+=1
            else:
                result_list.append((dummy_tuple,r))
                ir+=1
        elif il and not ir:
           result_list.append((l,dummy_tuple))
           il+=1
        else:
           result_list.append((dummy_tuple,r))
           ir+=1
    return result_list


#........................................................................
def send_mail(
    MESSAGE,
    SUBJECT = settings.SUBJECT,
    TOLIST  = settings.TOLIST,
    FROMADDR = settings.FROMADDR
):
    """ -----------------------------------------------------
    відправка повідомлення MESSAGE списку адресатів TOLIST
    від імені користувача settings.USER
    ----------------------------------------------------- """
    host = settings.MAIL_HOST
    port = settings.MAIL_PORT
    user = settings.MAIL_USER
    password = settings.MAIL_PASSWORD

    s = smtplib.SMTP()
    s.connect(host, port)
    s.ehlo()
    s.starttls()
    s.login(user, password)

    subject = SUBJECT
    fromaddr= FROMADDR
    tolist = TOLIST

    if isinstance(MESSAGE, unicode):
        message = MESSAGE
    elif isinstance(MESSAGE, str):
        message = unicode(MESSAGE, 'utf-8')
    else:
        message = str(MESSAGE)

    msg = MIMEText( message, _charset='utf-8')
    msg['Subject'] = subject
    msg['From']= fromaddr
    msg['To'] = ', '.join(tolist)
    s.sendmail(fromaddr, tolist, msg.as_string())
    s.quit()



#........................................................................
def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)


#........................................................................
def em(*args):
    """ -----------------------------------------------------------------------
    додає символьний рядок, створенний з вхідних параметрів функції, розділених
    через один пропуск (аналог дії команди print) в список error_log_list і
    повертає його з функції. Це потрібно, щоб вносити повідомлення в лог-файл і
    водночас зберігати його для відправки поштою (в разі виникнення помилки)
    ----------------------------------------------------------------------- """
    global error_log_list

    msg =  ' '.join([ str(arg) for arg in args])
    error_log_list.append(msg)
    return msg

# -------------------------------------------------------------------------
# last_retcode_filename - назва файлу, в якому зберігаємо останній код
# завершення скрипта. Якщо останній код був 1 (помилка), а зараз
# скрипт завершується успішно (0), відправляємо один "успішний" лист
# адміністратору(ам) після одного (або декількох) листів з помилками.
# Це дозволяє зрозуміти, що помилка була "автоматично" виправлена
# після серії ітерацій.
# -------------------------------------------------------------------------
def save_last_retcode(retcode):
    """ зберігає останній код повернення у файлі last_retcode_filename """
    with open(last_retcode_filename,'w') as f:
        print >>f, int(retcode)


#........................................................................
def get_last_retcode():
    """ читає останній код повернення з файлу last_retcode_filename """
    if os.path.isfile(last_retcode_filename):
        with open(last_retcode_filename, 'r') as f:
            retcode = f.read()
        return int(retcode)
    else:
        return 0


#..............................................................................
def init_log():
    global log_f
    try:
        log_f = open(log_file_name, 'a')
    except IOError, e:
        print em('ERROR: ', e)
        print em(e);
        print em('ERROR: Відсутнє вільне місце на диску, або невірні права доступу '\
              'до файлу `%s`.' % log_file_name)
        print em('ERROR: Перевірте, виправте і перезапустіть скрипт на виконання '\
                'ще раз.')
        exit_and_send_error_message()

#..............................................................................
def log(message):
    if LOG_LEVEL == 'INFO':
        if isinstance(message, unicode):
            message = unicode(message).encode('utf-8')
        cf = currentframe()
        filename = os.path.basename(getframeinfo(cf).filename)
        lineno = str(cf.f_back.f_lineno)
        dt_now = str(datetime.now())
        # друк в лог-файл:
        print >> log_f, dt_now, filename+':'+lineno, message
        # друк в консоль
        if LOG_TO_CON:
            print dt_now, filename+':'+lineno, message

#..............................................................................
def errlog(message):
    if isinstance(message, unicode):
        message = unicode(message).encode('utf-8')
    cf = currentframe()
    filename = os.path.basename(getframeinfo(cf).filename)
    lineno = str(cf.f_back.f_lineno)
    msg = em( str(datetime.now()), filename+':'+lineno, 'ERROR:',  message )
    # друк в лог-файл:
    print >> log_f, msg
    # друк в консоль
    if LOG_TO_CON:
        print msg


#..............................................................................
def exit_and_send_error_message(keep_running=False):
    """ -----------------------------------------------------------------------
    відправляє повідомлення про помилку електронною поштою і припиняє роботу
    скрипта з кодом повернення 1

    параметр: keep_running = True - не вилучати файл RUNNING перед завершенням
                                    роботи
    ----------------------------------------------------------------------- """
    global subject
    global error_log_list

    message = '\n'.join(error_log_list)
    try:
        send_mail(message, subject)
    except Exception as e:
        errlog('Невдача при відправці ел.пошти:')
        errlog('Exception: %s' %e)
        errlog('Ігноруємо відправку. :-(')

    retcode = 1
    save_last_retcode(retcode)

    # Якщо параметр keep_running не істинний, то вилучаємо файл-ознаку "RUNNING":
    if not keep_running and os.path.isfile(RUN_LABEL):
       os.remove(RUN_LABEL)

    sys.exit(retcode)


#..............................................................................
def read_csv(csv_file, delimiter = ';'):
    try:
        with file(csv_file, 'r') as f:
            csv_reader = csv.reader(f, delimiter=delimiter)
            result_list = [ row for row in csv_reader ]
    except IOError, e:
        errlog(e)
        errlog('Помилка читання файлу %s' % datapump_filename)
        errlog(
            'Ітерація не завершена. При '
            'наступному запуску буде спроба '
            'повторити цю ітерацію ще раз'
        )
        errlog('програма припинена з кодом помилки 1')
        exit_and_send_error_message()
    return result_list

#..............................................................................
def write_csv(rows, csv_file, delimiter=';'):
    class UnicodeWriter:
        """
            A CSV writer which will write rows to CSV file "f",
            which is encoded in the given encoding.
        """

        def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
            # Redirect output to a queue
            self.queue = cStringIO.StringIO()
            self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
            self.stream = f
            self.encoder = codecs.getincrementalencoder(encoding)()

        def writerow(self, row):
            self.writer.writerow([unicode(s).encode("utf-8") for s in row])
            # Fetch UTF-8 output from the queue ...
            data = self.queue.getvalue()
            data = data.decode("utf-8")
            # ... and reencode it into the target encoding
            data = self.encoder.encode(data)
            # write to the target stream
            self.stream.write(data)
            # empty queue
            self.queue.truncate(0)

        def writerows(self, rows):
            for row in rows:
                self.writerow(row)

    with open(csv_file, 'w') as f:
        writer = UnicodeWriter(f, delimiter=delimiter)
        writer.writerows(rows)

#..............................................................................
def load_sql(cursor, query):
    try:
        t1=time.time()
        cursor.execute(query)
        rows = cursor.fetchall()
        t2=time.time()
        log('SQL ЗАПИТ `%s...` відпрацював за %s sec' %
            (query[:20], str(timedelta(seconds=t2-t1)))
        )
    except pyodbc.ProgrammingError, e:
        errlog(e)
        errlog(
            'помилка вивантаження даних по '
            'запиту:\n'+query
        )
        errlog(
            'Ітерація не завершена. При '
            'наступному запуску буде спроба '
            'повторити цю ітерацію ще раз'
        )
        errlog('Програма припинена з кодом помилки 1')
        exit_and_send_error_message()

    return rows

#----------------------------------------------------------------------
# REST API: BEGIN /////////////////////////////////////////////////////
#----------------------------------------------------------------------
#
# ------------------------------------------------------------------------------
# функція POST(url, data, key_id, secret), яка:
#  1. підписує запит з новими даними (структура на мові Python (dict,list)),
#  2. надсилає його на сервер
#  3. отримує відповідь
#  4. обробляє код помилки
#  5. перетворює json-об'єкт відповіді в структури на мові Python (dict,list)
#  6. повертає відповідь
# ------------------------------------------------------------------------------
def POST(url, data, key_id, secret):
    # список заголовків, значення яких буде підписано:
    signature_headers = ['(request-line)', 'accept', 'date', 'host']

    # власне, заголовки:
    headers = {
      'Host': 'bi.datawiz.io',
      'Accept': 'application/json',
      'Date': "Mon, 17 Feb 2014 06:11:05 GMT",
      'content-type': 'application/json'
    }

    # 1. підписaти запит
    # --------------------
    auth = HTTPSignatureAuth(
        key_id = key_id,
        secret = secret,
        algorithm = 'hmac-sha256',
        headers = signature_headers
    )

    # 2. надiслати його на сервер і 3. отримувати відповідь
    # -----------------------------------------------------
    try:
        response = requests.post(
            url,
            data=json.dumps(data),
            auth=auth,
            headers=headers
        )
    except Exception as e:
        errlog('URL: ' + url)
        errlog('Не вдалось надіслати запит на сервер')
        errlog('Exception: %s' % e)
        result = {}
        result['status_code'] = requests.codes.BAD
        result['reason'] = e
        result['content'] = ''
        return (ERROR, result)

    # 4.обробити код помилки:
    # ------------------------
    if response.status_code not in (requests.codes.CREATED, requests.codes.OK):
        errlog('URL: ' + url)
        errlog('POST response.status_code: %s' % response.status_code)
        errlog('POST response.reason: %s' % response.reason)
        errlog('POST response.content: """'+response.content+'"""')

        # перевіряємо, чи отримане тіло відповіді типу 'json' i не порожнє:
        if response.headers.get('content-type') == 'application/json' and response.text:
            # перетворюємо json-текст відповіді в структури мови Python:
            result = response.json()
            # якщо в словнику `result` є ключ "detail" - друкуємо його значення:
            if 'detail' in result:
                errlog('DETAIL: ' + result['detail'])
            else:
                errlog('DETAIL: ' + str(result))
        else:
            result = {}
        if isinstance(result, list):
            result = { 'detail': result }

        result['status_code'] = response.status_code
        result['reason'] = response.reason
        result['content'] = response.content

        return (ERROR, result)

    # 5. перетворити json-об'єкт в структури на мові Python (dict,list)
    # -------------------------------------------------------------------
    if response.text:
        result = response.json()
    else:
        # якщо сервер повернув порожній json-об'єкт, повертаємо порожній словник:
        result = {}

    # 6. повернути відповідь
    # ----------------------
    return (OK, result)

#----------------------------------------------------------------------
# REST API: END ///////////////////////////////////////////////////////
#----------------------------------------------------------------------


