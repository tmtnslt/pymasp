import sqlite3
from datetime import datetime, timezone
import asyncio
import numpy as np
import io


_conn = None
_cur = None


def adapt_array(arr):
    """
    Adapt numpy for sqlite3. Taken from
    http://stackoverflow.com/a/31312102/190597 (SoulNibbler)
    :param arr:
    :return:
    """
    out = io.BytesIO()
    np.save(out, arr)
    out.seek(0)
    return sqlite3.Binary(out.read())


def convert_array(text):
    out = io.BytesIO(text)
    out.seek(0)
    return np.load(out)

# Register our conversions
sqlite3.register_adapter(np.ndarray, adapt_array)
sqlite3.register_converter("array", convert_array)


def open(db_path):
    _conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    _conn.row_factory= sqlite3.Row
    _cur = _conn.cursor()


def create_new_db():
    _cur.execute('''CREATE TABLE experiments
                    (id INTEGER PRIMARY KEY, date timestamp, title TEXT, assoc_job INT)''')
    _cur.execute('''CREATE TABLE jobs
                    (id INTEGER PRIMARY KEY, parent_id INT, date timestamp, title TEXT, note TEXT, assoc_data INT)''')
#    _cur.execute('''CREATE TABLE jobs_to_data #deprecated
#                    (parent_id INT, associated_id INT, isjob BOOLEAN)''')
    _cur.execute('''CREATE TABLE parameter_and_job
                    (parent_id INT, param_value REAL, assoc_job INT)''')
    _cur.execute('''CREATE TABLE data
                    (id INTEGER PRIMARY KEY, job_id INT, data BLOB)''')
    _cur.execute('''CREATE TABLE labbook
                    (date timestamp, note TEXT)''')
#    _cur.execute('''CREATE TABLE saved_experiments
#                    (title TEXT, pickle BLOB)''')

"""
UPDATE DATABASE
"""


def add_job(parent=None, title=None, json_meta=None, data_id=None):
    _cur.execute('''INSERT INTO jobs(parent_id, date, title, note, assoc_data) VALUES (?, ?, ?, ? , ?)''',
                 (parent, datetime.utcnow(),title, json_meta, data_id))

    return _cur.lastrowid


def update_job(job_id, parent_id=None, title=None, json_meta=None, assoc_data=None):
    cmd_str = '''Update jobs SET '''
    val=()
    if parent_id:
        cmd_str = cmd_str + '''parent_id=?, '''
        val = val + (parent_id,)
    if title:
        cmd_str = cmd_str + '''title=?, '''
        val = val + (title,)
    if json_meta:
        cmd_str = cmd_str + '''note=?, '''
        val = val + (json_meta,)
    if assoc_data:
        cmd_str = cmd_str + '''assoc_data=?, '''
        val = val + (assoc_data,)
    cmd_str = cmd_str + '''WHERE id=?'''
    _cur.execute(cmd_str, val)
    #_cur.execute('''UPDATE jobs SET date=?, title=?, note=? WHERE id=?''', (datetime.utcnow(),title,json_meta,job_id))


def assign_parameter_to_job(job_id, parameter_value, parent_id=None):
    """
    Assign a scalar value to a job. If you need to store more than a scalar double, use the json note of the job
    :param parent_id:
    :param job_id:
    :param parameter_value:
    :return:
    """
    _cur.execute('''INSERT INTO parameter_and_job(parent_id, param_value, assoc_job) VALUES (?,?,?)''',(parent_id,parameter_value,job_id))


def add_data(data, job_id=None):
    _cur.execute('''INSERT INTO data(job_id, data), VALUES (?,?)''',job_id, data)
    return _cur.lastrowid

#DEPRECATED
#def add_children(parent,children):
#    for ch in children:
#        _cur.execute('''INSERT INTO jobs_to_data(parent_id, associated_id, isjob) VALUES (?, ?, ?)''', (parent, ch, True))


def add_experiment(title, root_job):
    _cur.execute('''INSERT INTO experiments(date, title, assoc_job) VALUES(?, ?, ?)''', (datetime.utcnow(),title,root_job))

"""
QUERY DATABASE
"""


def list_experiments(time_range=None):
    if time_range:
        _cur.execute('''SELECT * FROM experiments WHERE date >= ? AND data <= ?''',
                     datetime.fromtimestamp(time_range,timezone.utc))
    else:
        _cur.execute('''SELECT * FROM experiments''')
    rows = _cur.fetchall()
    return rows


def list_jobs(time_range=None):
    if time_range:
        _cur.execute('''SELECT * FROM jobs WHERE date >= ? AND data <= ?''',
                     datetime.fromtimestamp(time_range,timezone.utc))
    else:
        _cur.execute('''SELECT * FROM jobs''')
    return _cur.fetchall()


def get_job_children(job_id):
    _cur.execute('''SELECT * FROM jobs WHERE parent_id = ?''',(job_id,))
    return _cur.fetchall()


def get_job_details(job_id):
    _cur.execute('''SELECT * FROM jobs WHERE id = ?''',(job_id,))
    return _cur.fetchall()


def get_data(data_id):
    _cur.execute('''SELECT * FROM data WHERE id= ?''',(data_id,))
    return _cur.fetchall()

