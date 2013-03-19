
import couchdb
import os

def openOrCreateDb(server, name):
    try:
        db = server.create(name) # newly created
        return db
    except couchdb.http.PreconditionFailed:
        db = server[name]
        return db

def saveObjectToCouch(db, o):
    if '_id' not in o:
        o['_id'] = o['id_str']
    try:
        db.save(o, batch='ok')
        return True
    except couchdb.http.ResourceConflict:
        logging.exception('Object with _id %s already in db; continuing...' % o['_id'])
        return False

def loadVitals(db, file_ref):
    name = os.path.basename(file_ref).split('.')[0]
    return db.get(name, default = {})

def updateVitals(db, file_ref, vitals):
    name = os.path.basename(file_ref).split('.')[0]
    vitals["_id"] = name
    try:
        return db.save(vitals)
    except couchdb.http.ResourceConflict:
        logging.exception('Object with _id %s already in db; continuing...' % vitals['_id'])
        return False