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
const helper = require('sendgrid').mail;
var AWS = require('aws-sdk');
const dotenv = require('dotenv');
const fs = require('fs');
const cron = require("node-cron");
const exec = require('child_process').exec;
require ('newrelic');
 
const database={
  host:process.env.HOST,
  port:process.env.PORT_DB,
  user:process.env.USER_DB,
  password:process.env.PASSWORD_DB,
  database:process.env.DATABASE
};

AWS.config.update({
    region: 'us-east-1',
    accessKeyId: process.env.ACCESS_KEY_ID,
    secretAccessKey: process.env.SECRET_ACCESS_KEY
});

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
var task = cron.schedule("* * * * *", function() {
    console.log("running a task every 1 minute");

    const sqs = new AWS.SQS({
        accessKeyId: process.env.ACCES_KEY_ID,
        secretAccessKey: process.env.SECRET_ACCESS_KEY
    });

    sqs.receiveMessage({
        AttributeNames: [
            "SentTimestamp"
        ],
        MaxNumberOfMessages: 1,
        MessageAttributeNames: [
            "All"
        ],
        QueueUrl: 'https://sqs.us-east-2.amazonaws.com/962103057762/SQS-Deployment-D',
        VisibilityTimeout: 20,
        WaitTimeSeconds: 0
    }, function(err, data) {
      	if (err) {
            console.log("Receive Error", err);
      	} else if (data.Messages) {
            console.log(data.Messages[0].Body);
    	    var objectQueue = JSON.parse(data.Messages[0].Body);
            var objectDeleteQueue = data.Messages[0].ReceiptHandle;
    	    pool.query('SELECT name, last_name, original_video, contest_id, email, id from videos WHERE status <> "Convertido" limit 1', function(err,res){
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
                    	let filePath = "uploads/" + fileName;
                    	console.log(filePath);
                    	console.log(viid); 
                    	let filePathConverted = "uploads/" + fileName.split('.')[0] + ".mp4";
                    	console.log(filePathConverted);

                        AWS.config.update({
                            region: "us-east-1",
                            endpoint: null,
                            accessKeyId: process.env.ACCESS_KEY_ID,
                            secretAccessKey: process.env.SECRET_ACCESS_KEY
                        });

                        const s3 = new AWS.S3({
                            accessKeyId: process.env.ACCESS_KEY_ID,
                            secretAccessKey: process.env.SECRET_ACCESS_KEY
                        });

                        s3.getObject({
                            Bucket: 's3-bucket-uniandes-d',
                            Key: objectQueue.name_video,
                        }, function(err, data) {
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
                                            AWS.config.update({
                                                region: "us-east-1",
                                                endpoint: null,
                                                accessKeyId: process.env.ACCESS_KEY_ID,
                                                secretAccessKey: process.env.SECRET_ACCESS_KEY
                                            });

                                            const s3 = new AWS.S3({
                                                accessKeyId: process.env.ACCESS_KEY_ID,
                                                secretAccessKey: process.env.SECRET_ACCESS_KEY
                                            });

                                            var dt = new Date();
                                            var nameEndField = dt.getTime() + "-" + fileName.split('.')[0]+'.mp4';
                                            const fileContent = fs.readFileSync(filePathConverted);
                                            s3.upload({
                                                Bucket: 's3-bucket-uniandes-d',
                                                Key: nameEndField,
                                                Body: fileContent
                                            }, function(err, data) {
                                                if (err) {
                                                    throw err;
                                                }
                                                console.log('File uploaded successfully');
                                            });

                                            let fileNameConv=fileName.split('.')[0]+'.mp4';
                                            let status = "Convertido";
console.log(fileNameConv + "=========================================================================");
                                            if(fileNameConv !== "load") {
                                                setTimeout(function(){
                                                    AWS.config.update({
                                                        region: "us-east-2",
                                                        endpoint: "http://dynamodb.us-east-2.amazonaws.com",
                                                        accessKeyId: process.env.ACCESS_KEY_ID, 
                                                        secretAccessKey: process.env.SECRET_ACCESS_KEY
                                                    });
                                                    var docClient = new AWS.DynamoDB.DocumentClient();
                                                    let modify = function () {
                                                        docClient.update({
                                                            TableName: "videos",
                                                            Key: { "email": emailVideo },
                                                            UpdateExpression: "set status_video = :statusBy, converted_video = :convertedVideoBy",
                                                            ExpressionAttributeValues: {
                                                                ":statusBy" : "Convertido",
                                                                ":convertedVideoBy" : nameEndField
                                                            },
                                                            ReturnValues: "UPDATED_NEW"
                                                        }, function (err, data) {
                                                            if (err) {
                                                                console.log("191 -> users::update::error - " + JSON.stringify(err, null, 2));
                                                            } else {
                                                                console.log("193 -> users::update::success "+JSON.stringify(data) );

                                                                sqs.deleteMessage({
                                                                    QueueUrl: 'https://sqs.us-east-2.amazonaws.com/962103057762/SQS-Deployment-D',
                                                                    ReceiptHandle: objectDeleteQueue
                                                                }, function(err, data) {
                                                                    if (err) {
                                                                        console.log("Delete Error", err);
                                                                    } else {
                                                                        console.log("Message Deleted", data);

                                                                        const sqsS = new AWS.SQS({
                                                                            accessKeyId: process.env.ACCESS_KEY_ID,
                                                                            secretAccessKey: process.env.SECRET_ACCESS_KEY
                                                                        });
                                                                        sqsS.sendMessage({
                                                                            MessageBody: JSON.stringify({
                                                                                order_id: new Date().getTime(),
                                                                                date_video: (new Date()).toISOString(),
                                                                                name_video: nameEndField,
                                                                                contest_id: contestid,
                                                                                file_name_conv: fileNameConv
                                                                            }),
                                                                            QueueUrl: 'https://sqs.us-east-2.amazonaws.com/962103057762/SQS-Response-D'
                                                                        }, (err, data) => {
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
                                                    var mail = new helper.Mail(
                                                        new helper.Email('yc.espejo10@uniandes.edu.co'), 
                                                        'Video procesado', 
                                                        new helper.Email(emailVideo), 
                                                        new helper.Content('text/plain', "Hola " + nameMail + " " + lastNameMail + " Tu video se proceso sin problemas " + nameEndField)
                                                    );

                                                    var sg = require('sendgrid')(process.env.SENDGRID_API_KEY);

                                                    var request = sg.emptyRequest({
                                                        method: 'POST',
                                                        path: '/v3/mail/send',
                                                        body: mail.toJSON(),
                                                    });

                                                    sg.API(request, function(error, response) {
                                                        console.log("----- email process begin -----");
                                                        console.log(response);
                                                        console.log(error);
                                                        console.log("----- email process end -----");
                                                    });
                                                }, 200);
                                            }

                                            pool.query('UPDATE videos set status = ?, converted_video = ? WHERE id = ?',[status,nameEndField,viid], function(errores,respuesta){
                                                if(errores){
                                                    throw errores;
                                                } else {
                                                    console.log(respuesta);
                                                    pool.query('SELECT * from contest WHERE id = ?',[contestid], function(error, result){
                                                        if(error){
                                                            throw error
                                                        } else {
                                                            console.log("Actualizado ");
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
