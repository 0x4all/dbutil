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
        console.log("mysql release:",key)
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

MySQLInstance.prototype.query = function(sql, cb) {
    this.pool.getConnection(function(err, conn){  
        if(err){  
            cb(err, null, null);  
        }else{  
            conn.query(sql,function(err, vals, fields){  
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