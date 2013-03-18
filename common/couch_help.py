
import couchdb

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
