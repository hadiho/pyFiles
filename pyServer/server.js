var express         = require('express');
var app             = express();
var bodyParser = require('body-parser');
const mysql = require('mysql');
const MySQLEvents = require('@rodrigogs/mysql-events');   
const cors = require('cors');
app.use(cors());

var server = require('http').Server(app);

const io = require('socket.io')(server, {
  cors: {
    origin: 'http://194.5.175.58:8000',
    credentials: true
  }
});

app.use(require("express").static('data'));
app.use(bodyParser.urlencoded({
  extended: true
}));
app.use(bodyParser.json());

var connectCounter = 0;

// This is auto initiated event when Client connects to Your Machine.  
io.on('connection',function(socket){

  connectCounter++;
  console.log("socket is on ",connectCounter);

  // socket.emit('latest_score',"Hi From Socket");
  emitAnEvent2("s")

  //When user dissconnects from server.
  socket.on('disconnect',function(){
    connectCounter--;
    io.emit('exit',{message:socket.nickname});
  });

});


server.listen(8000,function(){
    console.log("Listening on port 8000");
});


console.log(MySQLEvents.STATEMENTS);
const program = async () => {
  const connection = mysql.createConnection({
    host: '194.5.175.58',
    user: 'root',
    password: 'Hadi2150008140@$&!',
  });

  const instance = new MySQLEvents(connection, {
    startAtEnd: true,
    
  });

  await instance.start();

  instance.addTrigger({
    name: 'TEST',
    expression: '*',
    statement: MySQLEvents.STATEMENTS.ALL,
    onEvent: (event) => { // You will receive the events here
      
     
      if (event.table == 'hot_money'){
        console.log(event);
        console.log(event.affectedRows);

        emitAnEvent("update_hot_money",event.affectedRows)
        
      }

      if (event.table == 'last_price'){
        console.log(event);
        console.log(event.affectedRows);

        emitAnEvent("update_detail", event.affectedRows)
        
      }

      if (event.table == 'main_index'){
        console.log(event);
        console.log(event.affectedRows);

        emitAnEvent("update_main_index",event.affectedRows)

      }

      if (event.table == 'currency'){
        console.log(event);
        console.log(event.affectedRows);

        emitAnEvent("update_table1",event.affectedRows)

      }


    },
  });
  
  instance.on(MySQLEvents.EVENTS.CONNECTION_ERROR, console.error);
  instance.on(MySQLEvents.EVENTS.ZONGJI_ERROR, console.error);
};

program()
  .then(() => console.log('Waiting for database events...'))
  .catch(console.error);


  function emitAnEvent(channel, event){

    io.emit(channel,event)
    
  }

  function emitAnEvent2(event){
   
    var ss = [
      {
        after: {
          state: 'open',
          b_index: '1,250,233.87',
          index_change: -10552,
          index_change_percent: 3,
          index_h: '437,218.59',
          index_h_change: -1154,
          index_h_change_percent: -0.25999999046325684,
          market_value: '49,953,431.315B',
          trade_number: '231,622',
          trade_value: '20,868.766B',
          trade_volume: '1.775B'
        },
        before: {
          state: 'open',
          b_index: '1,250,233.87',
          index_change: -10552,
          index_change_percent: 2,
          index_h: '437,218.59',
          index_h_change: -1154,
          index_h_change_percent: -0.25999999046325684,
          market_value: '49,953,431.315B',
          trade_number: '231,622',
          trade_value: '20,868.766B',
          trade_volume: '1.775B'
        }
      }
    ]
    io.emit("update_main_index",ss)
    
  }
