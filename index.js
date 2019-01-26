/**
 * 管理当前服务的所有 mysql实例，以及mysql 连接
 * mysql实例：以为 host:port:database;为key 对应1个 mysql实例
 *      如果已经该实例就使用已有实例，否则创建1个实例
 */
var mysql = require("mysql");

var db = {};
module.exports = db;

db._instances = {};

db.mysql = function(dbconf) {
    var key = dbconf.host + ":" + dbconf.port + ":" + dbconf.database;
    if(!!db._instances[key]) {
        return db._instances[key];
    }
    var inst = new MySQLInstance(dbconf);
    db._instances[key] = inst;
    return inst;
}

db.mysql_close = function(dbconf, cb) {
    var key = dbconf.host + ":" + dbconf.port + ":" + dbconf.database;
    var inst = db._instances[key];
    if(!!inst) {
        delete db._instances[key];
        inst.close(cb);
    }
    else{
        if(cb && typeof cb == "function") {
            cb();
        }
    }
}

var MySQLInstance = function(dbconf){
    this.pool = mysql.createPool(dbconf);
    console.log("mysql started.");
};

MySQLInstance.prototype.query = function(sql, args, cb) {
    this.pool.getConnection(function(err, conn){  
        if(err){  
            cb(err, null, null);  
        }else{  
            conn.query(sql, args,function(err, vals, fields){  
                conn.release();  
                cb(err, vals, fields);  
            });  
        }  
    }); 
}

MySQLInstance.prototype.close = function(cb){
    this.pool.end((err)=>{
        console.log("mysql closed.",err ? err : "");
        if(cb && typeof cb == "function") {
            cb(err);
        }
    });
}


var updateSql = function(tablename, info, byname, callback){
    var sql = "UPDATE "+tablename+" SET ";
    var sqlargs = [];
    var prefix = "";
    for(var o in info) {
        if(info.hasOwnProperty(o) && o!=byname){
            sqlargs.push(info[o]);
            sql += prefix + o + "=?"
            prefix = ",";
        }
    }
    if(sqlargs.length == 0) {
        callback({type:"sys",errmsg:"no fields offer when update db." + tablename},null);
        return;
    }
    sql += " where "+byname+"=?";
    sqlargs.push(info[byname]);
    this.query(sql,sqlargs, function(err, rows, fields) {
        if (err) {
            err.type ="db";
            callback(err, null);
            return;
        }
        callback(null, info);
    });

}

var findSql = function(tablename, byname, byvalue, callback){
    var sql = "SELECT * from " + tablename + " where " + byname + "=?";
    this.query(sql,[byvalue], function(err, rows, fields) {
        if (err) {
            err.type = "db";
            callback(err);
            return;
        }
        if(rows.length  == 1) {
            callback(null,rows[0]);
            return;
        }
        callback(null, rows);
    });
}

var createSql = function(tablename, info, callback){
    var sql = "INSERT INTO "+ tablename +"(";
    var prefix= "";
    var params = ") VALUES(";
    var args = [];
    for(var o in info){
        sql += prefix + o; //uuid
        params += prefix + "?";
        args.push(info[o]);
        prefix = ","
    }
    sql += params + ")";
    this.query(sql,args, function(err, rows, fields) {
        if (err) {
            err.type = "db";
            callback(err, 0);
            return;
        }
        callback(null, info);
    });
}

var replaceSql = function(tablename, info, callback){
    var sql = "REPLACE INTO "+ tablename +"(";
    var prefix= "";
    var params = ") VALUES(";
    var args = [];
    for(var o in info){
        sql += prefix + o; //uuid
        params += prefix + "?";
        args.push(info[o]);
        prefix = ","
    }
    sql += params + ")";
    this.query(sql,args, function(err, rows, fields) {
        if (err) {
            err.type = "db";
            callback(err, 0);
            return;
        }
        callback(null, info);
    });
}

var deleteSql = function(tablename, byname, byvalue, callback){
    var sql = "DELETE FROM " + tablename + " where " + byname + "=?";
    this.query(sql,[byvalue],function(err, rows, fields){
        if (err) {
            err.type = "db";
            callback(err, 0);
            return;
        }
        callback(null, true);
    })
}

db.has_db_err = function(err, callback){
    if(err) {
        err.type = "db";
        callback( err );
        return true;
    }
    return false;
}

db.has_no_result = function(rows, callback){
    if(rows.length == 0 ){
        callback(null, null);
        return true;
    }
    return false;
}

MySQLInstance.prototype.createSql = createSql;
MySQLInstance.prototype.updateSql = updateSql;
MySQLInstance.prototype.deleteSql = deleteSql;
MySQLInstance.prototype.findSql = findSql;
MySQLInstance.prototype.replaceSql = replaceSql;




// var test = {
//     query: function(sql, args, cb) { console.log(sql, args); if(cb){cb(null, sql +"," + JSON.stringify(args))}},
//     createSql: createSql,
//     updateSql: updateSql
// }

// var tablename = "t_mj_rooms";
// var info = {id: 1001, name:"test", time:Date.now(),host:"12012",port:100};

// test.createSql(tablename, info, ()=>{});
// test.updateSql(tablename, info, "id",()=>{});