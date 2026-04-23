/**
 * 测试脚本：连接游戏服务器
 */
require("dotenv").config({ path: "D:\\db\\Github\\APP\\cheshi\\.env" });
const { GameClient } = require("./lib/gameClient");
const { bon, encode, parse, getEnc } = require("./lib/bonProtocol");

async function main() {
  const tokenData = {
    roleToken: "rUKzwTGESK/uFElqzE68NK34bOU9q2U6e+Rckaum9a6UWmOs3AY1kRboBgN2EGHNKXgTD0hBpXH8PMVNYcnO65RzAXoWolPfBRJfpH+hoDrWthZubYmgx2+eFF18ZQscy5rU20QsIos26AA+vLc1Jvp+5xH0CLCpozMO8kJd6YE=",
    roleId: 99925587,
    sessId: "177682785484202",
    connId: "1776827854844",
    isRestore: 0,
    name: "乌米-0-109660213"
  };

  // 构造 WS URL
  const wsUrl = `wss://xxz-xyzw.hortorgames.com/agent?p=${encodeURIComponent(
    JSON.stringify({
      roleToken: tokenData.roleToken,
      roleId: tokenData.roleId,
      sessId: tokenData.sessId,
      connId: tokenData.connId,
      isRestore: tokenData.isRestore
    })
  )}&e=x&lang=chinese`;

  console.log("WS URL:", wsUrl.replace(/roleToken=[^&]+/, "roleToken=***"));

  const client = new GameClient(tokenData, wsUrl);

  try {
    console.log("\n=== 1. 连接服务器 ===");
    await client.connect(15000);

    console.log("\n=== 2. 获取角色信息 ===");
    const roleInfo = await client.sendWithPromise("role_getroleinfo", {}, 8000);
    console.log("角色信息:", JSON.stringify(roleInfo, null, 2));

    console.log("\n=== 3. 测试签到 ===");
    try {
      const r = await client.sendWithPromise("system_signinreward", {}, 8000);
      console.log("签到结果:", JSON.stringify(r, null, 2));
    } catch (e) {
      console.log("签到失败:", e.message);
    }

    console.log("\n=== 4. 测试领取挂机奖励 ===");
    try {
      const r = await client.sendWithPromise("system_claimhangupreward", {}, 8000);
      console.log("挂机奖励结果:", JSON.stringify(r, null, 2));
    } catch (e) {
      console.log("挂机奖励失败:", e.message);
    }

    console.log("\n=== 5. 测试爬塔 ===");
    try {
      const tower = await client.sendWithPromise("tower_getinfo", {}, 8000);
      console.log("塔信息:", JSON.stringify(tower, null, 2));
      await new Promise(r => setTimeout(r, 500));
      const claim = await client.sendWithPromise("tower_claimreward", {}, 8000);
      console.log("塔奖励:", JSON.stringify(claim, null, 2));
    } catch (e) {
      console.log("爬塔失败:", e.message);
    }

    console.log("\n=== 全部测试完成 ===");
  } catch (err) {
    console.error("错误:", err.message);
  } finally {
    client.disconnect();
    process.exit(0);
  }
}

main();
