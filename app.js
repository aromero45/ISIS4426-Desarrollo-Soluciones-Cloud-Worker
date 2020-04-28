const express = require('express');
const morgan = require('morgan');
const exphbs = require('express-handlebars');
const path = require('path');
const flash = require('connect-flash');
const session = require('express-session');
const MySQLStore = require('express-mysql-session');
const passport = require('passport');
const bodyParser = require('body-parser');
const validator = require('express-validator');
const fileUpload = require('express-fileupload');
var AWS = require('aws-sdk');
const dotenv = require('dotenv');
const fs = require('fs');
const cron = require("node-cron");
const exec = require('child_process').exec;

dotenv.config( {path: "./environments/aws.env"});

const database={
  host:process.env.HOST,
  port:process.env.PORT_DB,
  user:process.env.USER_DB,
  password:process.env.PASSWORD_DB,
  database:process.env.DATABASE
};

AWS.config.update({
    region: 'us-east-1',
    accessKeyId:process.env.ACCES_KEY_ID,
    secretAccessKey:process.env.SECRET_ACCESS_KEY
});

const RUTA_GESTOR_ARCHIVOS = process.env.ruta_gestion_archivos;
const pool = require('./database.js');
const app = express();

app.set('port', process.env.PORT || 3000);

app.use(morgan('dev'));
app.use(express.urlencoded({extended: false})); 
app.use(express.json()); 
app.use(fileUpload());

app.use(session({
   secret: 'alex',
   resave: false,
   saveUninitialized: false,
   store: new MySQLStore(database)
}));

app.use(flash());

app.use ((req, res, next) =>{
  app.locals.success = req.flash('success');
  app.locals.message = req.flash('message');
  app.locals.user = req.user;  
  next();
});

app.use(express.static(path.join(__dirname, 'public')));

console.log("------------------------------------------------------------------------------------------------( " + ((new Date()).toISOString()) +  " )");
var task = cron.schedule("* * * * *", function() { //se ejecuta cada minuto
    console.log("running a task every 1 minute");

    const sqs = new AWS.SQS({
        accessKeyId:process.env.ACCES_KEY_ID,  // usuario nuevo para el sqs
        secretAccessKey:process.env.SECRET_ACCESS_KEY // clave nueva para usuario de aws
    });

    var queueURL = process.env.URL_SES; // ingreso el url del ses lo tengo en archivo .env
    var params = {
        AttributeNames: [
            "SentTimestamp"
        ],
        MaxNumberOfMessages: 1,
        MessageAttributeNames: [
            "All"
        ],
        QueueUrl: queueURL,
        VisibilityTimeout: 20,
        WaitTimeSeconds: 0
    };

    sqs.receiveMessage(params, function(err, data) {
      	if (err) {
            console.log("Receive Error", err);
      	} else if (data.Messages) {
            console.log(data.Messages[0].Body);
    	    var objectQueue = JSON.parse(data.Messages[0].Body);
            var objectDeleteQueue = data.Messages[0].ReceiptHandle;
    	    pool.query('SELECT name, last_name, original_video, contest_id, email, id from videos WHERE status <> "Convertido"', function(err,res){
                if(err){
             	    throw err;
                } else {
                    console.log(res);
                    for(ind in res){
                    	var contestid=res[ind].contest_id;
                    	var emailVideo=res[ind].email; 
                    	var viid=res[ind].id; 
                        var nameMail=res[ind].name; 
                        var lastNameMail=res[ind].last_name; 
                    	let fileName= res[ind].original_video;
                    	let filePath = RUTA_GESTOR_ARCHIVOS + fileName; // tengo que ver que hace este archivo y donde convierte el archivo
                    	console.log(filePath);
                    	console.log(viid); 
                    	let filePathConverted = RUTA_GESTOR_ARCHIVOS + fileName.split('.')[0]+'.mp4';
                    	console.log(filePathConverted);

                        var awsConfig3 = {
                            region: "us-east-1",
                            endpoint: null,
                            accessKeyId:process.env.ACCES_KEY_ID,  // usuario nuevo para el sqs
                            secretAccessKey:process.env.SECRET_ACCESS_KEY // clave nueva para usuario de aws
                        };
                        AWS.config.update(awsConfig3);
                        const s3 = new AWS.S3({
                            accessKeyId:process.env.ACCES_KEY_ID,  // usuario nuevo para el sqs
                            secretAccessKey:process.env.SECRET_ACCESS_KEY // clave nueva para usuario de aws
                        });

                        const paramsF = {
                            Bucket: process.env.URL_S3,  // direccion del S3 la puso en el .env
                            Key: objectQueue.name_video,
                        };
                        s3.getObject(paramsF, function(err, data) {
                            if (err) {
                                throw err;
                            }
                            fs.writeFileSync(filePath, data.Body);
                            console.log('File get object successfully');

                            fs.readFile(filePath, function(err,data){
                                console.log("File buffer: ", data)
                                if(err){
                                    throw err;
                                } else {
                                    console.log('ffmpeg -y -i ' + filePath +' '+filePathConverted);
                                    exec('ffmpeg -y -i ' + filePath +' '+filePathConverted,function (error, stdout, stderr) {
                                        console.log("Convirtiendo");
                                        console.log(stdout);
                                        if (error !== null) {
                                            console.log('exec error: ' + error);
                                        } else {
                                            const awsConfig5 = {
                                                region: "us-east-1",
                                                endpoint: null,
                                                accessKeyId:process.env.ACCES_KEY_ID,  // usuario nuevo para el sqs
                                                secretAccessKey:process.env.SECRET_ACCESS_KEY // clave nueva para usuario de aws
                                            };
                                            AWS.config.update(awsConfig5);

                                            const s3 = new AWS.S3({
                                                accessKeyId:process.env.ACCES_KEY_ID,  // usuario nuevo para el sqs
                                                secretAccessKey:process.env.SECRET_ACCESS_KEY // clave nueva para usuario de aws
                                            });

                                            var dt = new Date();
                                            var nameEndField = dt.getTime() + "-" + fileName.split('.')[0]+'.mp4';
                                            const fileContent = fs.readFileSync(filePathConverted);
                                            const paramsF = {
                                                Bucket: process.env.URL_S3, // s3 bucket estaba dentro de '' si algo ocurre volverlo a poner como antes
                                                Key: nameEndField,
                                                Body: fileContent
                                            };
                                            s3.upload(paramsF, function(err, data) {
                                                if (err) {
                                                    throw err;
                                                }
                                                console.log('File uploaded successfully');
                                            });

                                            let fileNameConv=fileName.split('.')[0]+'.mp4';
                                            let status = "Convertido";

                                            setTimeout(function(){
                                                const awsConfig = {
                                                    "region": "us-east-2",
                                                    "endpoint": process.env.ENDPOINT_DYNAMO, // configuracion de la base de datos dynamo para worker 
                                                    "accessKeyId": process.env.ACCES_KEY_ID2, 
                                                    "secretAccessKey": process.env.SECRET_ACCESS_KEY2
                                                };
                                                AWS.config.update(awsConfig);
                                                var docClient = new AWS.DynamoDB.DocumentClient();
                                                let modify = function () {
                                                    var params = {
                                                        TableName: "videos",
                                                        Key: { "email": emailVideo },
                                                        UpdateExpression: "set status_video = :statusBy, converted_video = :convertedVideoBy",
                                                        ExpressionAttributeValues: {
                                                            ":statusBy" : "Convertido",
                                                            ":convertedVideoBy" : nameEndField
                                                        },
                                                        ReturnValues: "UPDATED_NEW"
                                                    };
                                                    docClient.update(params, function (err, data) {
                                                        if (err) {
                                                            console.log("191 -> users::update::error - " + JSON.stringify(err, null, 2));
                                                        } else {
                                                            console.log("193 -> users::update::success "+JSON.stringify(data) );
                                                            var deleteParams = {
                                                                QueueUrl: queueURL,
                                                                ReceiptHandle: objectDeleteQueue
                                                            };
                                                            sqs.deleteMessage(deleteParams, function(err, data) {
                                                                if (err) {
                                                                    console.log("Delete Error", err);
                                                                } else {
                                                                    console.log("Message Deleted", data);

                                                                    const sqsS = new AWS.SQS({
                                                                        accessKeyId:process.env.ACCES_KEY_ID,  // usuario nuevo para el sqs
                                                                        secretAccessKey:process.env.SECRET_ACCESS_KEY // clave nueva para usuario de aws
                                                                    });
                                                                    const paramsQ = {
                                                                        MessageBody: JSON.stringify({
                                                                            order_id: new Date().getTime(),
                                                                            date_video: (new Date()).toISOString(),
                                                                            name_video: nameEndField,
                                                                            contest_id: contestid,
                                                                            file_name_conv: fileNameConv
                                                                        }),
                                                                        QueueUrl: process.env.URL_SES
                                                                    };
                                                                    sqsS.sendMessage(paramsQ, (err, data) => {
                                                                        if (err) {
                                                                            console.log("Error", err);
                                                                        } else {
                                                                            console.log("Successfully added message", data.MessageId);
                                                                            console.log("------------------------------------------------------------------------------------------------( " + ((new Date()).toISOString()) +  " )");
                                                                        }
                                                                    });
                                                                }
                                                            });
                                                        }
                                                    });
                                                }
                                                modify();
                                            }, 1000);

                                            setTimeout(function(){
                                                const ses = new AWS.SES({
                                                    accessKeyId:process.env.ACCES_KEY_ID,  // usuario nuevo para el sqs
                                                    secretAccessKey:process.env.SECRET_ACCESS_KEY // clave nueva para usuario de aws
                                                });
                                                const awsConfig2 = {
                                                    region: "us-east-1",
                                                    endpoint: null,
                                                    accessKeyId:process.env.ACCES_KEY_ID,  // usuario nuevo para el sqs
                                                    secretAccessKey:process.env.SECRET_ACCESS_KEY // clave nueva para usuario de aws
                                                };
                                                AWS.config.update(awsConfig2);
                                                const paramsM = {
                                                    Destination: { 
                                                        ToAddresses: [emailVideo]
                                                    },
                                                    Message: {
                                                        Body: {
                                                            Html: {
                                                                Charset: "UTF-8",
                                                                Data: "<html><body><h1>Hola " + nameMail + " " + lastNameMail + "</h1><p style='color:red'>Tu video se proceso sin problemas </p> <p>" + nameEndField + "</p></body></html>"
                                                            },
                                                            Text: {
                                                                Charset: "UTF-8",
                                                                Data: "Hola " + nameMail + " " + lastNameMail + " Tu video se proceso sin problemas " + nameEndField
                                                            }
                                                        },
                                                        Subject: {
                                                            Charset: "UTF-8",
                                                            Data: "Video procesado"
                                                        }
                                                    },
                                                    Source: "yeismer@minka.io"
                                                };
                                                const sendEmail = ses.sendEmail(paramsM).promise();
                                                sendEmail.then(data => {
                                                    console.log("email submitted to SES", data);
                                                }).catch(error => {
                                                    console.log(error);
                                                });
                                            }, 200);

                                            pool.query('UPDATE videos set status = ?, converted_video = ? WHERE id = ?',[status,nameEndField,viid], function(errores,respuesta){
                                                if(errores){
                                                    throw errores;
                                                } else {
                                                    console.log(respuesta);
                                                    pool.query('SELECT * from contest WHERE id = ?',[contestid], function(error, result){
                                                        if(error){
                                                            throw error
                                                        } else {
                                                            let urlvideo = result[0].url; 
                                                        }
                                                    });
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        });
                    }
                }
            });
      	}
    });
});

app.get('/stop', (req,res) => {
    task.stop();
    res.send('cron detenido');
})

app.get('/start', (req,res) => {
    task.start();
    res.send('cron iniciado');
})

app.listen(app.get('port'), () => {
    console.log('Server en puerto', app.get('port'));
});
