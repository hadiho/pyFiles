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
    origin: 'http://localhost:8000',
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
  //emitAnEvent2("s")

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
    host: 'localhost',
    user: 'root',
    password: 'Hadi2150008140@$&!',
  });

  const instance = new MySQLEvents(connection, {
    startAtEnd: true,
    serverId: 3,
    excludedSchemas: {
      mysql: true,
    },
  });

  await instance.start();

  instance.addTrigger({
    name: 'update main index',
    expression: 'temp.main_index',
    statement: MySQLEvents.STATEMENTS.UPDATE,
    onEvent: async (event) => {


      // if (event.table == 'hot_money'){
      //   console.log(event);
      //   console.log(event.affectedRows);
      //
      //   emitAnEvent("update_hot_money",event.affectedRows)
      //
      // }
      //
      // if (event.table == 'last_price'){
      //   console.log(event);
      //   console.log(event.affectedRows);
      //
      //   emitAnEvent("update_detail", event.affectedRows)
      //
      // }

      if (event.table == 'main_index'){
        console.log(event);
        console.log(event.affectedRows);

        emitAnEvent("update_main_index",event.affectedRows)

      }

      // if (event.table == 'currency'){
      //   console.log(event);
      //   console.log(event.affectedRows);
      //
      //   emitAnEvent("update_table1",event.affectedRows)
      //
      // }


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
    // var ss = [
    //   {
    //     after: {
    //       TIME: '12:23:02',
    //       NAME: 'سحاد',
    //       FULLNAME: 'هادی۲',
    //       CLOSE: 1200,
    //       PERCENT: 2,
    //       AVERAGE: 1000,
    //       TOTAL: 13000,
    //       NUMBER: 2,
    //       ATTRIBUTE: 2,
    //       TYPE: 2
    //     },
    //     before: undefined
    //   }
    // ]

    // var ss =[
    //   {
    //     after: {
    //       name: 'وسپهر',
    //       market: 'فرابورس',
    //       instance_code: '114312662654155',
    //       namad_code: 'IRO3SAIZ0001',
    //       industry_code: 56,
    //       industry: 'سرمایه گذاریها',
    //       state: 'مجاز',
    //       full_name: 'سرمايه گذاري مالي سپهرصادرات',
    //       first_price: 10100,
    //       yesterday_price: 10100,
    //       close_price: 1200,
    //       close_price_change: 2,
    //       close_price_change_percent: 0.01,
    //       final_price: 10100,
    //       final_price_change: 0,
    //       final_price_change_percent: 1.1125369292536007e-308,
    //       eps: '1236',
    //       free_float: '',
    //       highest_price: 10105,
    //       lowest_price: 10100,
    //       daily_price_high: 10706,
    //       daily_price_low: 9898,
    //       'P:E': 8.17,
    //       trade_number: '12324',
    //       trade_volume: '30836760',
    //       trade_value: '311459680561',
    //       all_stocks: '30000000000',
    //       basis_volume: '11881189',
    //       real_buy_volume: '6736100',
    //       co_buy_volume: '24100660',
    //       real_sell_volume: '30824368',
    //       co_sell_volume: '12392',
    //       real_buy_value: '68034610000',
    //       co_buy_value: '243416666000',
    //       real_sell_value: '311326116800',
    //       co_sell_value: '125159200',
    //       real_buy_count: '1231',
    //       co_buy_count: '4',
    //       real_sell_count: '10518',
    //       co_sell_count: '4',
    //       sell_count1: '5',
    //       sell_count2: '3',
    //       sell_count3: '19',
    //       buy_count1: '1499',
    //       buy_count2: '4',
    //       buy_count3: '3',
    //       sell_price1: '10103',
    //       sell_price2: '10105',
    //       sell_price3: '10106',
    //       buy_price1: '10100',
    //       buy_price2: '10099',
    //       buy_price3: '10090',
    //       sell_volume1: '3343',
    //       sell_volume2: '2448',
    //       sell_volume3: '31229',
    //       buy_volume1: '276522191',
    //       buy_volume2: '2496',
    //       buy_volume3: '6465',
    //       market_value: '303000000000000'
    //     },
    //     before: {
    //       name: 'وسپهر',
    //       market: 'فرابورس',
    //       instance_code: '114312662654155',
    //       namad_code: 'IRO3SAIZ0001',
    //       industry_code: 56,
    //       industry: 'سرمایه گذاریها',
    //       state: 'مجاز',
    //       full_name: 'سرمايه گذاري مالي سپهرصادرات',
    //       first_price: 10100,
    //       yesterday_price: 10100,
    //       close_price: 1342,
    //       close_price_change: 2,
    //       close_price_change_percent: 0.01,
    //       final_price: 10100,
    //       final_price_change: 0,
    //       final_price_change_percent: 1.1125369292536007e-308,
    //       eps: '1236',
    //       free_float: '',
    //       highest_price: 10105,
    //       lowest_price: 10100,
    //       daily_price_high: 10706,
    //       daily_price_low: 9898,
    //       'P:E': 8.17,
    //       trade_number: '12324',
    //       trade_volume: '30836760',
    //       trade_value: '311459680561',
    //       all_stocks: '30000000000',
    //       basis_volume: '11881189',
    //       real_buy_volume: '6736100',
    //       co_buy_volume: '24100660',
    //       real_sell_volume: '30824368',
    //       co_sell_volume: '12392',
    //       real_buy_value: '68034610000',
    //       co_buy_value: '243416666000',
    //       real_sell_value: '311326116800',
    //       co_sell_value: '125159200',
    //       real_buy_count: '1231',
    //       co_buy_count: '4',
    //       real_sell_count: '10518',
    //       co_sell_count: '4',
    //       sell_count1: '5',
    //       sell_count2: '3',
    //       sell_count3: '19',
    //       buy_count1: '1499',
    //       buy_count2: '4',
    //       buy_count3: '3',
    //       sell_price1: '10103',
    //       sell_price2: '10105',
    //       sell_price3: '10106',
    //       buy_price1: '10100',
    //       buy_price2: '10099',
    //       buy_price3: '10090',
    //       sell_volume1: '3343',
    //       sell_volume2: '2448',
    //       sell_volume3: '31229',
    //       buy_volume1: '276522191',
    //       buy_volume2: '2496',
    //       buy_volume3: '6465',
    //       market_value: '303000000000000'
    //     }
    //   }
    // ]

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