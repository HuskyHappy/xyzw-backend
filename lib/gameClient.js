/**
 * XYZW 游戏客户端 - Node.js 简化版
 * 基于 xyzwWebSocket.js 改造，适配 ws + CommonJS
 */
const WebSocket = require("ws");
const { g_utils, bon, encode, parse, getEnc } = require("./bonProtocol");

// 错误码映射
const errorCodeMap = {
  700010: "任务未达成完成条件",
  1400010: "没有购买月卡",
  12000116: "今日已领取免费奖励",
  3300060: "扫荡条件不满足",
  200400: "操作太快",
  2300190: "今天已签到",
  1000020: "今天已领取",
  700020: "已领取过",
  400010: "物品不足",
  1500020: "能量不足",
  3500020: "没有可领取的奖励",
  12000050: "发车次数达上限",
  12000060: "不在发车时间",
  200160: "模块未开启",
  200020: "出了点小问题",
};

// 预注册命令模板
const CMD_DEFAULTS = {
  role_getroleinfo: {
    clientVersion: "2.21.2-fa918e1997301834-wx",
    inviteUid: 0,
    platform: "hortor",
    platformExt: "mix",
    scene: "",
  },
  system_claimhangupreward: {},
  system_signinreward: {},
  task_claimdailyreward: { rewardId: 0 },
  task_claimdailypoint: { taskId: 1 },
  system_getdatabundlever: { isAudit: false },
  system_buygold: { buyNum: 1 },
  system_custom: { key: "", value: 0 },
  tower_getinfo: {},
  tower_claimreward: {},
  evotower_getinfo: {},
  evotower_fight: {},
  evotower_claimreward: {},
  arena_startarea: {},
  arena_getareatarget: { refresh: false },
  arena_getarearank: {},
  item_openbox: { itemId: 2001, number: 10 },
  bottlehelper_start: { bottleType: -1 },
  bottlehelper_claim: {},
  car_getrolecar: {},
  car_refresh: {},
  car_claim: {},
  legacy_getinfo: {},
  legacy_claimhangup: {},
  hero_recruit: { byClub: false, recruitNumber: 1, recruitType: 3 },
  study_startgame: {},
  fight_startlevel: {},
  fight_starttower: {},
  fight_startboss: {},
  fight_startpvp: {},
  fight_startdungeon: {},
  matchteam_getroleteaminfo: {},
  bosstower_getinfo: {},
  bosstower_startboss: {},
  bosstower_startbox: {},
  legion_getinfo: {},
  legion_signin: {},
  mail_getlist: { category: [0, 4, 5], lastId: 0, size: 60 },
  mail_claimallattachment: { category: 0 },
  presetteam_getinfo: {},
  artifact_lottery: { lotteryNumber: 1, newFree: true, type: 1 },
  genie_sweep: { genieId: 1 },
  mergebox_getinfo: {},
  mergebox_claimfreeenergy: {},
  mergebox_openbox: {},
  discount_claimreward: { discountId: 1 },
  collection_claimfreereward: {},
};

// 响应→请求命令映射
const RESP_MAP = {
  role_getroleinforesp: "role_getroleinfo",
  system_claimhanguprewardresp: "system_claimhangupreward",
  system_signinrewardresp: "system_signinreward",
  system_getdatabundleverresp: "system_getdatabundlever",
  task_claimdailyrewardresp: "task_claimdailyreward",
  tower_getinforesp: "tower_getinfo",
  tower_claimrewardresp: "tower_claimreward",
  evotowerinforesp: "evotower_getinfo",
  evotower_fightresp: "evotower_fight",
  evotower_claimrewardresp: "evotower_claimreward",
  arena_startarearesp: "arena_startarea",
  arena_getareatargetresp: "arena_getareatarget",
  arena_getarearankresp: "arena_getarearank",
  item_openboxresp: "item_openbox",
  bottlehelper_claimresp: "bottlehelper_claim",
  bottlehelper_startresp: "bottlehelper_start",
  car_getrolecarresp: "car_getrolecar",
  car_refreshresp: "car_refresh",
  car_claimresp: "car_claim",
  legacy_getinforesp: "legacy_getinfo",
  legacy_claimhangupresp: "legacy_claimhangup",
  hero_recruitresp: "hero_recruit",
  studyresp: "study_startgame",
  fight_startlevelresp: "fight_startlevel",
  fight_starttowerresp: "fight_starttower",
  fight_startbossresp: "fight_startboss",
  fight_startpvpresp: "fight_startpvp",
  matchteam_getroleteaminforesp: "matchteam_getroleteaminfo",
  bosstower_getinforesp: "bosstower_getinfo",
  bosstower_startbossresp: "bosstower_startboss",
  bosstower_startboxresp: "bosstower_startbox",
  legion_getinforesp: "legion_getinfo",
  legion_signinresp: "legion_signin",
  mail_claimallattachmentresp: "mail_claimallattachment",
  presetteam_getinforesp: "presetteam_getinfo",
  artifact_lotteryresp: "artifact_lottery",
  genie_sweepresp: "genie_sweep",
  mergeboxinforesp: "mergebox_getinfo",
  mergebox_claimfreeenergyresp: "mergebox_claimfreeenergy",
  mergebox_openboxresp: "mergebox_openbox",
  discount_claimrewardresp: "discount_claimreward",
  collection_claimfreerewardresp: "collection_claimfreereward",
  // 通用同步响应
  syncrewardresp: ["system_buygold", "system_signinreward", "discount_claimreward"],
  syncpushresp: ["system_mysharecallback"],
};

class GameClient {
  /**
   * @param {object} tokenData - { roleToken, roleId, sessId, connId, isRestore }
   * @param {string} wsUrl - 可选，默认用默认地址
   */
  constructor(tokenData, wsUrl) {
    this.token = tokenData;
    this._wsUrl =
      wsUrl ||
      `wss://xxz-xyzw.hortorgames.com/agent?p=${encodeURIComponent(
        JSON.stringify({
          roleToken: tokenData.roleToken,
          roleId: tokenData.roleId,
          sessId: tokenData.sessId,
          connId: tokenData.connId,
          isRestore: tokenData.isRestore,
        }),
      )}&e=x&lang=chinese`;

    this.ws = null;
    this.connected = false;
    this.ack = 0;
    this.seq = 0;
    this.promises = {};
    this.heartbeatTimer = null;
    this.lastMsgAt = Date.now();
    this._log = (...args) =>
      console.log(`[${tokenData.name || "?"}]`, new Date().toISOString().slice(11, 19), ...args);
  }

  log(...args) {
    this._log(...args);
  }

  /** 连接游戏服务器 */
  connect(timeoutMs = 15000) {
    return new Promise((resolve, reject) => {
      this.log("连接中:", this._wsUrl.replace(/roleToken=[^&]+/, "roleToken=***"));

      let timer;
      if (timeoutMs) {
        timer = setTimeout(() => {
          if (this.ws) this.ws.close();
          reject(new Error("连接超时"));
        }, timeoutMs);
      }

      try {
        this.ws = new WebSocket(this._wsUrl);
      } catch (err) {
        clearTimeout(timer);
        reject(err);
        return;
      }

      this.ws.binaryType = "arraybuffer";

      this.ws.on("open", () => {
        this.connected = true;
        this.log("连接成功");
        clearTimeout(timer);
        this._startHeartbeat();
        resolve();
      });

      this.ws.on("message", (data) => {
        this._handleMessage(data);
      });

      this.ws.on("close", (code, reason) => {
        this.connected = false;
        this._stopHeartbeat();
        this.log("连接关闭:", code, reason.toString());
        // reject 所有等待中的 promise
        for (const id in this.promises) {
          this.promises[id].reject(new Error("连接已关闭"));
          delete this.promises[id];
        }
      });

      this.ws.on("error", (err) => {
        this.log("错误:", err.message);
        clearTimeout(timer);
        if (!this.connected) reject(err);
      });
    });
  }

  /** 断开连接 */
  disconnect() {
    this._stopHeartbeat();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.connected = false;
  }

  /** 发送命令（无等待） */
  send(cmd, params = {}) {
    if (!this.connected) {
      this.log("未连接，无法发送:", cmd);
      return;
    }

    const assignedSeq = cmd === "heart_beat" ? 0 : ++this.seq;

    // 构建原始消息（body 已经是 BON bytes）
    const defaultBody = CMD_DEFAULTS[cmd] || {};
    const mergedParams = { ...defaultBody, ...params };

    const rawMsg = {
      cmd,
      ack: this.ack,
      seq: assignedSeq,
      time: Date.now(),
      body: bon.encode(mergedParams), // body 用 BON 编码
    };

    // 整体 BON 编码 → 'x' 加密 → 发送
    const encoded = encode(rawMsg, getEnc("x"));
    this.ws.send(encoded);

    this.log("发送:", cmd, "seq:", assignedSeq);
    return assignedSeq;
  }

  /** 发送命令并等待响应 */
  sendWithPromise(cmd, params = {}, timeoutMs = 8000) {
    return new Promise((resolve, reject) => {
      if (!this.connected) {
        return reject(new Error("未连接"));
      }

      const requestSeq = ++this.seq;

      // 设置超时
      const timer = setTimeout(() => {
        delete this.promises[requestSeq];
        reject(new Error(`请求超时: ${cmd} (${timeoutMs}ms)`));
      }, timeoutMs);

      // 注册 Promise
      this.promises[requestSeq] = {
        resolve,
        reject,
        originalCmd: cmd,
        timer,
      };

      // 构建并发送消息
      const defaultBody = CMD_DEFAULTS[cmd] || {};
      const mergedParams = { ...defaultBody, ...params };

      const rawMsg = {
        cmd,
        ack: this.ack,
        seq: requestSeq,
        time: Date.now(),
        body: bon.encode(mergedParams),
      };

      const encoded = encode(rawMsg, getEnc("x"));
      this.ws.send(encoded);

      this.log("发送并等待:", cmd, "seq:", requestSeq);
    });
  }

  /** 批量执行多个命令（顺序，间隔 delayMs） */
  async executeBatch(commands, delayMs = 500) {
    const results = [];
    for (const { cmd, params, timeout } of commands) {
      try {
        const r = await this.sendWithPromise(cmd, params, timeout || 8000);
        results.push({ cmd, success: true, data: r });
        this.log("成功:", cmd);
      } catch (err) {
        results.push({ cmd, success: false, error: err.message });
        this.log("失败:", cmd, err.message);
      }
      if (delayMs > 0) await sleep(delayMs);
    }
    return results;
  }

  // ==================== 内部方法 ====================

  _handleMessage(data) {
    this.lastMsgAt = Date.now();

    let protoMsg;
    try {
      protoMsg = parse(data, getEnc("auto"));
    } catch (err) {
      this.log("解析消息失败:", err.message);
      return;
    }

    const msg = protoMsg._raw;
    if (!msg) return;

    // 更新 ack
    if (typeof msg.seq === "number" && msg.seq >= 0) {
      this.ack = msg.seq;
    }

    const respKey = msg.resp !== undefined ? msg.resp : msg.cmd;
    const respCmdKey = respKey?.toLowerCase?.() || respKey;

    // 优先用 resp 字段匹配
    if (msg.resp !== undefined && this.promises[msg.resp]) {
      const p = this.promises[msg.resp];
      clearTimeout(p.timer);
      delete this.promises[msg.resp];

      if (msg.code === 0 || msg.code === undefined) {
        const bodyData = protoMsg.rawData;
        p.resolve(bodyData || msg);
      } else {
        const desc = errorCodeMap[msg.code] || msg.hint || "未知错误";
        p.reject(new Error(`[${msg.code}] ${desc}`));
      }
      return;
    }

    // 兼容：响应→请求命令映射匹配
    let requestCmd = RESP_MAP[respCmdKey];
    if (!requestCmd) return;

    if (typeof requestCmd === "string") requestCmd = [requestCmd];

    for (const [id, p] of Object.entries(this.promises)) {
      if (requestCmd.includes(p.originalCmd)) {
        clearTimeout(p.timer);
        delete this.promises[id];

        if (msg.code === 0 || msg.code === undefined) {
          p.resolve(protoMsg.rawData || msg);
        } else {
          const desc = errorCodeMap[msg.code] || msg.hint || "未知错误";
          p.reject(new Error(`[${msg.code}] ${desc}`));
        }
        break;
      }
    }
  }

  _startHeartbeat() {
    // 3秒后发第一个心跳
    setTimeout(() => {
      if (this.connected) this._sendHeartbeat();
    }, 3000);

    // 之后每5秒一次
    this.heartbeatTimer = setInterval(() => {
      if (this.connected) this._sendHeartbeat();
    }, 5000);
  }

  _stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  _sendHeartbeat() {
    if (!this.connected) return;
    try {
      const rawMsg = {
        cmd: "_sys/ack",
        ack: this.ack,
        seq: 0,
        time: Date.now(),
        body: undefined,
      };
      const encoded = encode(rawMsg, getEnc("x"));
      this.ws.send(encoded);
    } catch (e) {
      // ignore
    }
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

module.exports = { GameClient };
