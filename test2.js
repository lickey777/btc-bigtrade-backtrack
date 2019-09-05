const bitcoin = require("bitcoinjs-lib")
const BitcoinRpc = require("bitcoin-rpc-promise");
const rpc = new BitcoinRpc("http://etl:Qtum100$@127.0.0.1:8332");

const blockId =  async function(){
    await rpc.call('getblockhash', i);
};
console.log(blockId);
