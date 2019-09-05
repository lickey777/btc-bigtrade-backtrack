const bitcoin = require("bitcoinjs-lib")
const BitcoinRpc = require("bitcoin-rpc-promise");
const rpc = new BitcoinRpc("http://etl:Qtum100$@127.0.0.1:8332");
const redis = require('redis'),
    RDS_PORT = 6379,
    RDS_HOST = '10.142.0.4',
    client = redis.createClient(RDS_PORT,RDS_HOST);

const mysql  = require('mysql');

const zmq = require('zeromq');
const sock = zmq.socket('sub');
const cors = require('cors')
const request = require('request');

const url = 'https://api.chainup.info/v1/tag/address/type/list'

sock.connect('tcp://127.0.0.1:28332')
sock.subscribe('rawtx')

const txArray = [];

const start = 565052,
      stop = 593152;



for(let i = start; i <= stop; i++) {
    const blockId =  async function(){
        await rpc.call('getblockhash', i);
    }
    const block = async function() {
        await rpc.call('getblock', blockId, 2);
    }
    const blockHeight = block.height;
    const blockTime = block.time;

    const bigTrade = async function() {
        await Promise.all(Array.from({ length: block.tx }, (item, key) => txBackTrack(block.tx[key])))
    }
}

async function txBackTrack(tx) {
    let count = 0;
    for(let i = 0; i < tx.vout.length; i++)
        count += tx.vout[i].value
    if (count < 10000000000)
        return [];
    const info = await getBigTrade(tx);
    if (info.success === false){
        console.log("2  --  未打上标签，判断不存在交易所相关的交易")
        return [];
    }
    console.log("3  --  确定是大额转账，打标签后确认")
    return checkFormat(info);
}



sock.on('message', async function (topic, message) {
    if (topic.toString() !== 'rawtx') {
        return
    }
    const tx = bitcoin.Transaction.fromHex(message.toString('hex'));
    let count = 0;
    let txOutLength = tx.outs.length
    for(let i = 0; i < txOutLength; i++)
        count += tx.outs[i].value
    if (count < 10000000000)
        return;
    const txid = tx.getId();
    for(let i = 0; i < txArray.length; i++){
        if(txArray[i] === txid)
            return;
    }
    txArray.push(txid);
    if(txArray.length > 2000)
        txArray.shift();
    console.log("1 -- 进入初次筛选的交易，检查是否有大额交易   " + txid);
    const date = Date.parse(new Date())/1000;
    const txDetail = await rpc.call('getrawtransaction', txid, true);  //  根据队列参数取得mempool中的交易数据
    //console.log('-----------');
    const info = await getBigTrade(txDetail);
    if (info.success === false){
        console.log("2  --  未打上标签，判断不存在交易所相关的交易" + txid)
        return;
    }
    console.log("3  --  确定是大额转账，打标签后确认" + txid)
    const Bigresult = checkFormat(info);
    if(Bigresult.length === 0){
        console.log("4  --  打完标签后发现 并不是大额转账");
        return;
    }
    console.log("5  --  打完标签后依然是大额转账，总共有   " + Bigresult.length + "   条大额转账写入redis")
    const connection = mysql.createConnection({
        host     : '35.237.143.230' ,
        user     : 'etl',
        password : 'nL6iP4aR3vWx3Lm',
        database: 'bitcoin_notify'
    });
    connection.connect();
    for(let i = 0; i < Bigresult.length; i++){
        Bigresult[i].date = date;
        if ((Bigresult[i].success) && (Bigresult[i].output_address.slice(0,2) !== 'bc')) {
            const addSql = 'INSERT INTO bitcoin_big_trade_monitor(id,type,date,txid,value,input_address,input_entity,input_address_type,output_address,output_entity,output_address_type) VALUES(0,?,?,?,?,?,?,?,?,?,?)';
            const  addSqlParams = [Bigresult[i].type, Bigresult[i].date, Bigresult[i].txid, Bigresult[i].value, Bigresult[i].input_address, Bigresult[i].input_entity, Bigresult[i].input_address_type, Bigresult[i].output_address, Bigresult[i].output_entity, Bigresult[i].output_address_type];
//增
            try {
                connection.query(addSql,addSqlParams,function (err, result) {
                    if(err){
                        console.log('[INSERT ERROR] - ',err.message);

                    } else {
                        console.log("成功将大额交易写入 mysql，数据库 ID 是 " + Bigresult[i].txid)
                        SendToRedis(Bigresult[i]);
                        console.log("成功写入redis，交易 ID 是 " + Bigresult[i].txid)
                    }
                });
            } catch (e) {
                console.log("写入数据库失败")
            }
        }
    }
    connection.end();
});

// 获取标签
async function getTag(value){
    const requestData = {
        addressList: []
    };
    requestData.addressList = value;
    let result = {}
    return await new Promise((resolve, reject) => (request(
        {
            url: url,
            method: "POST",
            json: true,
            headers: {
                "content-type": "application/json"
            },
            body: requestData
        }, (err, res, body) => {
            if (!err && res.statusCode === 200) {
                if(body.success === true){
                    result = body.data
                    //console.log(result)
                } else {
                    console.log("在请求时出错了")
                }
            }
            resolve(result)
        })))
}

async function getBigTrade(tx) {
    const vinObj = {},
        voutObj = {};

    const addrArray = [];
    const tempInfo = {};

    for(let i = 0; i < tx.vin.length; i++){
        let tempVin = await rpc.call('getrawtransaction', tx.vin[i].txid, true);
        let tempVinAddress = tempVin.vout[tx.vin[i].vout].scriptPubKey.addresses[0]
        if(vinObj.hasOwnProperty(tempVinAddress)){
            vinObj[tempVinAddress] += tempVin.vout[tx.vin[i].vout].value;
        } else {
            vinObj[tempVinAddress] = tempVin.vout[tx.vin[i].vout].value;
        }
    }
    for(let j = 0; j < tx.vout.length; j++){
        let asm = tx.vout[j].scriptPubKey.asm;
        if(asm.slice(0,9) !== 'OP_RETURN'){
            let value = Number(tx.vout[j].value);
            let tempVoutAddress = tx.vout[j].scriptPubKey.addresses[0];
            if(voutObj.hasOwnProperty(tempVoutAddress)){
                if (vinObj.hasOwnProperty(tempVoutAddress)) {
                    if ((value + voutObj[tempVoutAddress]) <= vinObj[tempVoutAddress]) {
                        vinObj[tempVoutAddress] = vinObj[tempVoutAddress] - value - voutObj[tempVoutAddress];
                        voutObj[tempVoutAddress] = 0;
                    } else {
                        voutObj[tempVoutAddress] += value - vinObj[tempVoutAddress];
                        vinObj[tempVoutAddress] = 0;
                    }
                } else {
                    voutObj[tempVoutAddress] += value;
                }
            } else {
                if (vinObj.hasOwnProperty(tempVoutAddress)) {
                    if(vinObj[tempVoutAddress] >= value){
                        vinObj[tempVoutAddress] -= value;
                    } else {
                        voutObj[tempVoutAddress] = value - vinObj[tempVoutAddress];
                        vinObj[tempVoutAddress] = 0;
                    }
                } else {
                    voutObj[tempVoutAddress] = value;
                }
            }
        }
    }

    for(let key in voutObj){
        if(voutObj[key] >= 100){
            addrArray.push(key);
        } else {
            delete (voutObj[key])
        }
    }
    if(Object.keys(voutObj).length === 0){
        tempInfo.success = false;
        return tempInfo;
    }

    for(let key in vinObj){
        if(vinObj[key] === 0){
            delete (vinObj[key])
        } else {
            addrArray.push(key)
        }
    }

    const addrTagObj = await getTag(addrArray)
    tempInfo.success = true;
    tempInfo.exist = addrTagObj["exist"]
    tempInfo.txId = tx.txid;
    tempInfo.VInTx = vinObj;
    tempInfo.VOutTx = voutObj;
    tempInfo.addrTagObj = addrTagObj;
    return tempInfo;
}


// 检查输入是否存在带'热钱包'标签的地址，并且检查是否是多个
function checkHot(value){
    let hotCount = 0;
    let addr = ''
    for(let key in value.VInTx){
        if ((value.addrTagObj[key].type === 0) || (value.addrTagObj[key].type === 1)){
            hotCount += 1;
            addr = key;
        }
        if (hotCount === 2)
            return {
                success: true,
                hotType: 2,
                addr: addr
            }
    }
    if (hotCount === 1)
        return {
            success: true,
            hotType: 1,
            addr: addr
        }
    else
        return {
            success: false,
            hotType: 0,
            addr: ''
        }
}

// 检查输入是否存在带'冷钱包'标签的地址
function checkCold(value) {
    for(let key in value.VInTx){
        if (value.addrTagObj[key].type === 2) {
            return {
                success: true,
                addr: key
            };
        }
    }
    return{
        success: false,
        addr: ''
    }
}

// 检查的属于哪一类大额交易
function checkFormat(info){
    let finalResult = [];
    if (info.exist) {
        for(let key in info.VOutTx) {
            if (info.addrTagObj[key].entity === -1) {
                let checkRes = checkHot(info)
                if (checkRes.success) {
                    if(checkRes.hotType === 1){
                        const bigTrade = {
                            success: true,
                            type: 2,
                            date: 0,
                            block: 0,
                            txid: info.txId,
                            value: info.VOutTx[key],
                            input_address: checkRes.addr,
                            input_entity: info.addrTagObj[checkRes.addr].entity,
                            input_address_type: info.addrTagObj[checkRes.addr].type,
                            output_address: key,
                            output_entity: info.addrTagObj[key].entity,
                            output_address_type: info.addrTagObj[key].type
                        };
                        finalResult.push(bigTrade);
                    } else {
                        const bigTrade = {
                            success: true,
                            type: 3,
                            date: 0,
                            block: 0,
                            txid: info.txId,
                            value: info.VOutTx[key],
                            input_address: 'multi',
                            input_entity: info.addrTagObj[checkRes.addr].entity,
                            input_address_type: -1,
                            output_address: key,
                            output_entity: info.addrTagObj[key].entity,
                            output_address_type: info.addrTagObj[key].type
                        };
                        finalResult.push(bigTrade);
                    }
                }
            }
            if (info.addrTagObj[key].type === 1) {
                let checkRes = checkCold(info)
                if (checkRes.success && info.addrTagObj[checkRes.addr].entity === info.addrTagObj[key].entity) {
                    const bigTrade = {
                        success: true,
                        type: 6,
                        date: 0,
                        block: 0,
                        txid: info.txId,
                        value: info.VOutTx[key],
                        input_address: checkRes.addr,
                        input_entity: info.addrTagObj[checkRes.addr].entity,
                        input_address_type: info.addrTagObj[checkRes.addr].type,
                        output_address: key,
                        output_entity: info.addrTagObj[key].entity,
                        output_address_type: info.addrTagObj[key].type
                    };
                    finalResult.push(bigTrade);
                }
            }
            if (info.addrTagObj[key].type === 0) {
                let hotCount = 0;
                let unknowCount = 0;
                let tempAddr = '';
                for (let i in info.VInTx) {
                    if (((info.addrTagObj[i].type === 1) || (info.addrTagObj[i].type === 0)) && (info.addrTagObj[key].entity !== info.addrTagObj[i].entity)) {
                        hotCount ++;
                        tempAddr = i;
                    }
                    if (hotCount > 1) {
                        const bigTrade = {
                            success: true,
                            type: 5,
                            date: 0,
                            block: 0,
                            txid: info.txId,
                            value: info.VOutTx[key],
                            input_address: 'multi',
                            input_entity: info.addrTagObj[tempAddr].entity,
                            input_address_type: -1,
                            output_address: key,
                            output_entity: info.addrTagObj[key].entity,
                            output_address_type: info.addrTagObj[key].type
                        };
                        finalResult.push(bigTrade);
                        break;
                    }
                }
                if (hotCount === 1) {
                    const bigTrade = {
                        success: true,
                        type: 4,
                        date: 0,
                        block: 0,
                        txid: info.txId,
                        value: info.VOutTx[key],
                        input_address: tempAddr,
                        input_entity: info.addrTagObj[tempAddr].entity,
                        input_address_type: info.addrTagObj[tempAddr].type,
                        output_address: key,
                        output_entity: info.addrTagObj[key].entity,
                        output_address_type: info.addrTagObj[key].type
                    };
                    finalResult.push(bigTrade);
                } else if(hotCount === 0) {
                    for (let j in info.VInTx) {
                        if (info.addrTagObj[j].type === -1) {
                            unknowCount ++;
                            tempAddr = j;
                        }
                        if(unknowCount > 1) {
                            const bigTrade = {
                                success: true,
                                type: 1,
                                date: 0,
                                block: 0,
                                txid: info.txId,
                                value: info.VOutTx[key],
                                input_address: 'multi',
                                input_entity: info.addrTagObj[tempAddr].entity,
                                input_address_type: -1,
                                output_address: key,
                                output_entity: info.addrTagObj[key].entity,
                                output_address_type: info.addrTagObj[key].type
                            };
                            finalResult.push(bigTrade);
                            break;
                        }
                    }
                    if (unknowCount === 1) {
                        const bigTrade = {
                            success: true,
                            type: 0,
                            date: 0,
                            block: 0,
                            txid: info.txId,
                            value: info.VOutTx[key],
                            input_address: tempAddr,
                            input_entity: info.addrTagObj[tempAddr].entity,
                            input_address_type: info.addrTagObj[tempAddr].type,
                            output_address: key,
                            output_entity: info.addrTagObj[key].entity,
                            output_address_type: info.addrTagObj[key].type
                        };
                        finalResult.push(bigTrade);
                    }
                }
            }
        }
    } else {
        for(let key in info.VOutTx) {
            if(Object.keys(info.VOutTx).length === 1){
                const tempAddr = Object.keys(info.VOutTx)[0];
                const bigTrade = {
                    success: true,
                    type: 7,
                    date: 0,
                    block: 0,
                    txid: info.txId,
                    value: info.VOutTx[key],
                    input_address: tempAddr,
                    input_entity: info.addrTagObj[tempAddr].entity,
                    input_address_type: info.addrTagObj[tempAddr].type,
                    output_address: key,
                    output_entity: info.addrTagObj[key].entity,
                    output_address_type: info.addrTagObj[key].type
                };
                finalResult.push(bigTrade);
            } else {
                const bigTrade = {
                    success: true,
                    type: 8,
                    date: 0,
                    block: 0,
                    txid: info.txId,
                    value: info.VOutTx[key],
                    input_address: 'multi',
                    input_entity: -1,
                    input_address_type: -1,
                    output_address: key,
                    output_entity: info.addrTagObj[key].entity,
                    output_address_type: info.addrTagObj[key].type
                };
                finalResult.push(bigTrade);
            }
        }
    }
    return finalResult;
}


// publish 到 redis
function SendToRedis(info){
    if(info.success === true) {
        const bufferBody = JSON.stringify(info);
        const redisMsg = {
            type: 0,
            describe: ''
        };
        redisMsg.describe = bufferBody;
        const message = JSON.stringify(redisMsg);
        client.publish("btc-notification-channel", message, function (err,result) {
            if(err){
                console.log("error: " + err.toString())
            } else {
                console.log("publish bigTrade monitor to btc-notification-channel success");
                console.log("结束的时间是：" + Date.now())
            }
        });
        // client.lpush('btc-big-trade', bufferBody, function (err, result) {
        //     if (err) {
        //         console.log("error: " + err.toString())
        //     } else {
        //         console.log("set message 'big' in btc-big-data:    success")
        //     }
        // });
    }
}
