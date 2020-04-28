const mysql = require('mysql');
const { promisify } = require('util');



const database={
    host:process.env.HOST,
    port:process.env.PORT_DB,
    user:process.env.USER_DB,
    password:process.env.PASSWORD_DB,
    database:process.env.DATABASE
};

console.log(database);



const pool = mysql.createPool(database); 

pool.getConnection((err, connection) => {
 
     if(err) {

        if (err.code === 'PROTOCOL_CONNECTION_LOST'){
            console.error('DATABASE CONNECTION WAS CLOSED'); 
        }
        if (err.code === 'ER_CON_COUNT_ERROR'){
            console.error('DATABASE HAS TO MANY CONNECTIONS');
        }
        if (err.code ==='ECONNREFUSED') {
            console.error('DATABASE CONNECTION WAS REFUSED');
        }
     }

     if (connection) connection.release();
     console.log('DB is connected');
     return; 

}); 

// promesas a callbacks 
pool.query = promisify(pool.query);

module.exports = pool; 