/**
 * XYZW 游戏自动化后端 - Node.js + Supabase + Render
 * 支持定时任务执行，自动连接游戏服务器
 */
require("dotenv").config({ path: "D:\\db\\Github\\APP\\cheshi\\.env" });
const express = require("express");
const cors = require("cors");
const cron = require("node-cron");
const { createClient } = require("@supabase/supabase-js");
const { WebSocket } = require("ws");
const { GameClient } = require("./lib/gameClient");
const { bon, encode, parse, getEnc } = require("./lib/bonProtocol");

const app = express();
app.use(cors());
app.use(express.json());

// ==================== Supabase 客户端 ====================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// ==================== 任务定义 ====================
// task_type → 命令序列
const TASK_DEFINITIONS = {
  // 签到
  signIn: {
    name: "签到",
    commands: [{ cmd: "system_signinreward", params: {} }],
  },
  // 领取挂机奖励
  claimHangup: {
    name: "领取挂机奖励",
    commands: [{ cmd: "system_claimhangupreward", params: {} }],
  },
  // 领取日常任务奖励
  claimDaily: {
    name: "领取日常奖励",
    commands: [{ cmd: "task_claimdailyreward", params: { rewardId: 0 } }],
  },
  // 领取每日积分任务奖励
  claimDailyPoint: {
    name: "领取每日积分",
    commands: [{ cmd: "task_claimdailypoint", params: { taskId: 1 } }],
  },
  // 爬塔
  climbTower: {
    name: "爬塔领奖励",
    commands: [
      { cmd: "tower_getinfo", params: {} },
      { cmd: "tower_claimreward", params: {} },
    ],
  },
  // 怪异塔
  climbWeirdTower: {
    name: "怪异塔",
    commands: [
      { cmd: "evotower_getinfo", params: {} },
      { cmd: "evotower_claimreward", params: {} },
    ],
  },
  // 竞技场
  arenaFight: {
    name: "竞技场",
    commands: [
      { cmd: "arena_startarea", params: {} },
      { cmd: "arena_getareatarget", params: { refresh: false } },
    ],
  },
  // 领取邮件附件
  claimMail: {
    name: "领取邮件附件",
    commands: [{ cmd: "mail_claimallattachment", params: { category: 0 } }],
  },
  // 领取车辆
  claimCar: {
    name: "领取车辆",
    commands: [{ cmd: "car_getrolecar", params: {} }],
  },
  // 刷新车辆
  refreshCar: {
    name: "刷新车辆",
    commands: [{ cmd: "car_refresh", params: {} }],
  },
  // 功法挂机
  legacyHangup: {
    name: "功法挂机",
    commands: [{ cmd: "legacy_getinfo", params: {} }, { cmd: "legacy_claimhangup", params: {} }],
  },
  // 武将招募
  heroRecruit: {
    name: "武将招募",
    commands: [{ cmd: "hero_recruit", params: { byClub: false, recruitNumber: 1, recruitType: 3 } }],
  },
  // 学习问答
  studyGame: {
    name: "学习问答",
    commands: [{ cmd: "study_startgame", params: {} }],
  },
  // 灯神
  genieSweep: {
    name: "灯神",
    commands: [{ cmd: "genie_sweep", params: { genieId: 1 } }],
  },
  // 开箱
  openBox: {
    name: "开箱",
    commands: [{ cmd: "item_openbox", params: { itemId: 2001, number: 10 } }],
  },
  // 军团签到
  legionSignIn: {
    name: "军团签到",
    commands: [{ cmd: "legion_getinfo", params: {} }, { cmd: "legion_signin", params: {} }],
  },
  // 宝库
  bossTower: {
    name: "咸王宝库",
    commands: [
      { cmd: "bosstower_getinfo", params: {} },
      { cmd: "bosstower_startboss", params: {} },
      { cmd: "bosstower_startbox", params: {} },
    ],
  },
  // 合并魔盒
  mergeBox: {
    name: "合并魔盒",
    commands: [
      { cmd: "mergebox_getinfo", params: {} },
      { cmd: "mergebox_claimfreeenergy", params: {} },
      { cmd: "mergebox_openbox", params: {} },
    ],
  },
  // 一键日常（常用任务组合）
  dailyBundle: {
    name: "一键日常",
    commands: [
      { cmd: "system_signinreward", params: {} },
      { cmd: "system_claimhangupreward", params: {} },
      { cmd: "task_claimdailyreward", params: { rewardId: 0 } },
      { cmd: "tower_getinfo", params: {} },
      { cmd: "tower_claimreward", params: {} },
      { cmd: "mail_claimallattachment", params: { category: 0 } },
      { cmd: "legacy_getinfo", params: {} },
      { cmd: "legacy_claimhangup", params: {} },
    ],
  },

  // ==================== 前端 availableTasks 对应的任务 ====================
  // 日常任务（批量执行多个日常操作）
  startBatch: {
    name: "日常任务",
    commands: [
      { cmd: "system_signinreward", params: {} },
      { cmd: "system_claimhangupreward", params: {} },
      { cmd: "task_claimdailyreward", params: { rewardId: 0 } },
      { cmd: "mail_claimallattachment", params: { category: 0 } },
    ],
  },
  // 领取挂机奖励（领取 + 加钟 4 次）
  claimHangUpRewards: {
    name: "领取挂机",
    commands: [
      { cmd: "system_claimhangupreward", params: {} },
      { cmd: "system_mysharecallback", params: { isSkipShareCard: true, type: 2 } },
      { cmd: "system_mysharecallback", params: { isSkipShareCard: true, type: 2 } },
      { cmd: "system_mysharecallback", params: { isSkipShareCard: true, type: 2 } },
      { cmd: "system_mysharecallback", params: { isSkipShareCard: true, type: 2 } },
    ],
  },
  // 一键加钟（加钟 4 次）
  batchAddHangUpTime: {
    name: "一键加钟",
    commands: [
      { cmd: "system_mysharecallback", params: { isSkipShareCard: true, type: 2 } },
      { cmd: "system_mysharecallback", params: { isSkipShareCard: true, type: 2 } },
      { cmd: "system_mysharecallback", params: { isSkipShareCard: true, type: 2 } },
      { cmd: "system_mysharecallback", params: { isSkipShareCard: true, type: 2 } },
    ],
  },
  // 重置罐子（停止计时+开始计时）
  resetBottles: {
    name: "重置罐子",
    commands: [
      { cmd: "bottlehelper_stop", params: {} },
      { cmd: "bottlehelper_start", params: {} },
    ],
  },
  // 一键领取罐子
  batchlingguanzi: {
    name: "一键领取罐子",
    commands: [{ cmd: "bottlehelper_claim", params: {} }],
  },
  // 一键爬塔（使用已有的 climbTower）
  // 一键爬怪异塔（使用已有的 climbWeirdTower）
  // 一键答题
  batchStudy: {
    name: "一键答题",
    commands: [{ cmd: "study_startgame", params: {} }],
  },
  // 智能发车
  batchSmartSendCar: {
    name: "智能发车",
    commands: [{ cmd: "car_smartsend", params: {} }],
  },
  // 一键收车
  batchClaimCars: {
    name: "一键收车",
    commands: [{ cmd: "car_getrolecar", params: {} }],
  },
  // 批量开箱
  batchOpenBox: {
    name: "批量开箱",
    commands: [{ cmd: "item_openbox", params: { itemId: 2001, number: 10 } }],
  },
  // 按积分开箱
  batchOpenBoxByPoints: {
    name: "按积分开箱",
    commands: [{ cmd: "item_openboxbypoint", params: {} }],
  },
  // 领取宝箱积分
  batchClaimBoxPointReward: {
    name: "领取宝箱积分",
    commands: [{ cmd: "boxpoint_claimreward", params: {} }],
  },
  // 批量钓鱼
  batchFish: {
    name: "批量钓鱼",
    commands: [{ cmd: "fish_start", params: { rodId: 1 } }],
  },
  // 批量招募
  batchRecruit: {
    name: "批量招募",
    commands: [{ cmd: "hero_recruit", params: { byClub: false, recruitNumber: 1, recruitType: 3 } }],
  },
  // 一键宝库前3层
  batchbaoku13: {
    name: "一键宝库前3层",
    commands: [
      { cmd: "bosstower_getinfo", params: {} },
      { cmd: "bosstower_startboss", params: { floor: 1 } },
      { cmd: "bosstower_startboss", params: { floor: 2 } },
      { cmd: "bosstower_startboss", params: { floor: 3 } },
    ],
  },
  // 一键宝库4,5层
  batchbaoku45: {
    name: "一键宝库4,5层",
    commands: [
      { cmd: "bosstower_getinfo", params: {} },
      { cmd: "bosstower_startboss", params: { floor: 4 } },
      { cmd: "bosstower_startboss", params: { floor: 5 } },
    ],
  },
  // 一键梦境
  batchmengjing: {
    name: "一键梦境",
    commands: [{ cmd: "dream_sweep", params: {} }],
  },
  // 一键俱乐部签到
  batchclubsign: {
    name: "一键俱乐部签到",
    commands: [
      { cmd: "legion_getinfo", params: {} },
      { cmd: "legion_signin", params: {} },
    ],
  },
  // 一键竞技场战斗3次
  batcharenafight: {
    name: "一键竞技场战斗",
    commands: [
      { cmd: "arena_startarea", params: {} },
      { cmd: "arena_getareatarget", params: { refresh: false } },
      { cmd: "arena_getareatarget", params: { refresh: false } },
      { cmd: "arena_getareatarget", params: { refresh: false } },
    ],
  },
  // 一键钓鱼补齐
  batchTopUpFish: {
    name: "一键钓鱼补齐",
    commands: [{ cmd: "fish_topup", params: {} }],
  },
  // 一键竞技场补齐
  batchTopUpArena: {
    name: "一键竞技场补齐",
    commands: [{ cmd: "arena_topup", params: {} }],
  },
  // 一键领取怪异塔免费道具
  batchClaimFreeEnergy: {
    name: "一键领取怪异塔免费道具",
    commands: [{ cmd: "evotower_claimfreeenergy", params: {} }],
  },
  // 一键换皮闯关
  skinChallenge: {
    name: "一键换皮闯关",
    commands: [{ cmd: "skin_challenge", params: {} }],
  },
  // 一键购买四圣碎片
  legion_storebuygoods: {
    name: "一键购买四圣碎片",
    commands: [{ cmd: "legion_storebuygoods", params: { goodsId: 1 } }],
  },
  // 一键黑市采购
  store_purchase: {
    name: "一键黑市采购",
    commands: [{ cmd: "store_purchase", params: { storeId: 1, goodsId: 1 } }],
  },
  // 免费领取珍宝阁
  collection_claimfreereward: {
    name: "免费领取珍宝阁",
    commands: [{ cmd: "collection_claimfreereward", params: {} }],
  },
  // 批量领取功法残卷
  batchLegacyClaim: {
    name: "批量领取功法残卷",
    commands: [{ cmd: "legacy_getinfo", params: {} }, { cmd: "legacy_claimhangup", params: {} }],
  },
  // 批量赠送功法残卷
  batchLegacyGiftSendEnhanced: {
    name: "批量赠送功法残卷",
    commands: [{ cmd: "legacy_giftsend", params: {} }],
  },
  // 一键使用怪异塔道具
  batchUseItems: {
    name: "一键使用怪异塔道具",
    commands: [{ cmd: "item_use", params: { itemId: 3001, number: 1 } }],
  },
  // 一键怪异塔合成
  batchMergeItems: {
    name: "一键怪异塔合成",
    commands: [{ cmd: "item_merge", params: {} }],
  },
  // 一键领取蟠桃园任务
  batchClaimPeachTasks: {
    name: "一键领取蟠桃园任务",
    commands: [{ cmd: "peach_claimtasks", params: {} }],
  },
  // 一键扫荡灯神
  batchGenieSweep: {
    name: "一键扫荡灯神",
    commands: [{ cmd: "genie_sweep", params: { genieId: 1 } }],
  },
  // 一键购买梦境商品
  batchBuyDreamItems: {
    name: "一键购买梦境商品",
    commands: [{ cmd: "dream_buyitem", params: { itemId: 1 } }],
  },
};

// ==================== 日志存储（内存，保留最近500条） ====================
const LOG_MAX = 500;
const logs = [];

function addLog(level, category, message, meta = {}) {
  const entry = {
    ts: new Date().toISOString(),
    level,
    category,
    message,
    ...meta,
  };
  logs.unshift(entry);
  if (logs.length > LOG_MAX) logs.pop();
  console.log(`[${level}] [${category}] ${message}`, meta);
}

// ==================== Token 加载 ====================
async function loadTokens() {
  const { data, error } = await supabase.from("tokens").select("*").eq("enabled", true);
  if (error) {
    addLog("ERROR", "supabase", "加载tokens失败", { error: error.message });
    return [];
  }
  addLog("INFO", "supabase", `加载${data?.length || 0}个tokens`);
  return data || [];
}

// ==================== 任务执行引擎 ====================
async function executeTask(task) {
  addLog("INFO", "task", `开始执行任务: ${task.name}`, { taskId: task.id });

  // 解析 settings（包含模板设置和 batchSettings）
  let taskSettings = {};
  if (task.settings) {
    try {
      taskSettings = typeof task.settings === "string" ? JSON.parse(task.settings) : task.settings;
    } catch (e) {
      addLog("WARN", "task", "解析 settings 失败，使用默认值", { taskId: task.id });
    }
  }

  // 从 Supabase 获取最新的批量设置（实时读取，不依赖快照）
  let liveBatchSettings = {};
  try {
    const { data: bsData } = await supabase
      .from("app_settings")
      .select("*")
      .eq("key", "batch_settings")
      .single();
    if (bsData?.value) liveBatchSettings = bsData.value;
  } catch (e) {
    addLog("WARN", "task", "读取全局设置失败，使用任务快照", { taskId: task.id });
  }

  // 合并：任务 settings 中的 batchSettings 优先，回退到全局设置
  const bs = { ...liveBatchSettings, ...(taskSettings.batchSettings || {}) };
  const cmdDelay = bs.commandDelay || 800;
  const connTimeout = bs.connectionTimeout || 15000;

  // 如果有 templateId，从 Supabase 获取最新模板设置（实时读取）
  let liveTemplateSettings = {};
  const templateId = taskSettings.templateId;
  if (templateId) {
    try {
      const { data: tmpl } = await supabase
        .from("task_templates")
        .select("*")
        .eq("id", templateId)
        .single();
      if (tmpl?.settings) liveTemplateSettings = tmpl.settings;
      addLog("INFO", "task", `从 Supabase 加载模板: ${tmpl?.name || templateId}`, { taskId: task.id });
    } catch (e) {
      addLog("WARN", "task", `读取模板失败 (${templateId})，使用快照`, { taskId: task.id });
    }
  }

  // 合并：实时模板设置 优先，回退到任务 settings 中的模板字段
  const templateSettings = {
    arenaFormation: liveTemplateSettings.arenaFormation || taskSettings.arenaFormation || 1,
    towerFormation: liveTemplateSettings.towerFormation || taskSettings.towerFormation || 1,
    bossFormation: liveTemplateSettings.bossFormation || taskSettings.bossFormation || 1,
    bossTimes: liveTemplateSettings.bossTimes !== undefined ? liveTemplateSettings.bossTimes : (taskSettings.bossTimes !== undefined ? taskSettings.bossTimes : 2),
    claimBottle: liveTemplateSettings.claimBottle !== undefined ? liveTemplateSettings.claimBottle : (taskSettings.claimBottle !== false),
    claimHangUp: liveTemplateSettings.claimHangUp !== undefined ? liveTemplateSettings.claimHangUp : (taskSettings.claimHangUp !== false),
    arenaEnable: liveTemplateSettings.arenaEnable !== undefined ? liveTemplateSettings.arenaEnable : (taskSettings.arenaEnable !== false),
    openBox: liveTemplateSettings.openBox !== undefined ? liveTemplateSettings.openBox : (taskSettings.openBox !== false),
    claimEmail: liveTemplateSettings.claimEmail !== undefined ? liveTemplateSettings.claimEmail : (taskSettings.claimEmail !== false),
    blackMarketPurchase: liveTemplateSettings.blackMarketPurchase !== undefined ? liveTemplateSettings.blackMarketPurchase : (taskSettings.blackMarketPurchase !== false),
    payRecruit: liveTemplateSettings.payRecruit !== undefined ? liveTemplateSettings.payRecruit : (taskSettings.payRecruit !== false),
  };

  // 加载 selected_tokens（Supabase 存的是 UUID 列表）
  let tokenIds = task.selected_tokens;
  if (typeof tokenIds === "string") {
    try { tokenIds = JSON.parse(tokenIds); } catch { tokenIds = []; }
  }
  if (!Array.isArray(tokenIds) || tokenIds.length === 0) {
    addLog("WARN", "task", "没有选中的token，跳过", { taskId: task.id });
    return;
  }

  // 加载 selected_tasks（任务 ID 列表）
  let taskIds = task.selected_tasks;
  if (typeof taskIds === "string") {
    try { taskIds = JSON.parse(taskIds); } catch { taskIds = []; }
  }
  if (!Array.isArray(taskIds) || taskIds.length === 0) {
    addLog("WARN", "task", "没有选中的任务，跳过", { taskId: task.id });
    return;
  }

  // 加载所有可用 token
  const allTokens = await loadTokens();

  // 找匹配的 token
  const targetTokens = allTokens.filter((t) => tokenIds.includes(t.id));
  if (targetTokens.length === 0) {
    addLog("WARN", "task", "没有找到匹配的tokens", { taskId: task.id });
    return;
  }

  addLog("INFO", "task", `将执行于 ${targetTokens.length} 个角色`, { taskId: task.id });

  const taskResults = [];

  for (const token of targetTokens) {
    // 解析 token 数据（Supabase 存的是 JSON 字符串）
    let tokenData;
    try {
      tokenData = typeof token.token === "string" && token.token.startsWith("{")
        ? JSON.parse(token.token)
        : { roleToken: token.token, roleId: token.roleId, sessId: token.sessId, connId: token.connId, isRestore: token.isRestore };
    } catch (err) {
      addLog("ERROR", "task", `Token解析失败: ${token.name}`, { error: err.message });
      continue;
    }

    tokenData.name = token.name;

    const client = new GameClient(tokenData, token.ws_url);

    try {
      // 连接（使用 settings 中的超时时间）
      await client.connect(connTimeout);

      // 先获取角色信息
      try {
        await client.sendWithPromise("role_getroleinfo", {}, 8000);
        await sleep(500);
      } catch (e) {
        addLog("WARN", "task", `获取角色信息失败 (${token.name})`, { error: e.message });
      }

      // 逐个执行选中的任务
      const cmdResults = [];
      for (const tid of taskIds) {
        // startBatch（日常任务）动态读取模板设置
        if (tid === "startBatch") {
          const cmds = [];
          // 签到（无条件执行）
          cmds.push({ cmd: "system_signinreward", params: {} });
          // 领取挂机
          if (templateSettings.claimHangUp) {
            cmds.push({ cmd: "system_claimhangupreward", params: {} });
          }
          // 领取日常奖励
          cmds.push({ cmd: "task_claimdailyreward", params: { rewardId: 0 } });
          // 竞技场
          if (templateSettings.arenaEnable) {
            cmds.push({ cmd: "batcharenafight", params: { formationId: templateSettings.arenaFormation } });
          }
          // 爬塔
          cmds.push({ cmd: "climbTower", params: { formationId: templateSettings.towerFormation } });
          // Boss战
          if (templateSettings.bossTimes > 0) {
            for (let i = 0; i < templateSettings.bossTimes; i++) {
              cmds.push({ cmd: "climbTower_challengeboss", params: { formationId: templateSettings.bossFormation } });
            }
          }
          // 领罐子
          if (templateSettings.claimBottle) {
            cmds.push({ cmd: "bottlehelper_claim", params: {} });
          }
          // 开宝箱
          if (templateSettings.openBox) {
            cmds.push({ cmd: "item_openbox", params: { itemId: 2001, number: 10 } });
          }
          // 领取邮件
          if (templateSettings.claimEmail) {
            cmds.push({ cmd: "mail_claimallattachment", params: { category: 0 } });
          }
          // 付费招募
          if (templateSettings.payRecruit) {
            cmds.push({ cmd: "payrecruit_recruitone", params: {} });
          }
          // 黑市购买
          if (templateSettings.blackMarketPurchase) {
            cmds.push({ cmd: "store_buyblackmarketitem", params: {} });
          }
          addLog("INFO", "task", `[${token.name}] startBatch 动态命令: ${cmds.length} 条`, { taskId: task.id });
          const results = await client.executeBatch(cmds, cmdDelay);
          cmdResults.push(...results);
          continue;
        }

        const taskDef = TASK_DEFINITIONS[tid];
        if (!taskDef) {
          addLog("WARN", "task", `未知任务: ${tid} (${token.name})`, { taskId: task.id });
          cmdResults.push({ cmd: tid, success: false, error: "未知任务" });
          continue;
        }
        // 使用 settings 中的命令延迟
        const results = await client.executeBatch(taskDef.commands, cmdDelay);
        cmdResults.push(...results);
      }

      // 记录结果
      for (const r of cmdResults) {
        const logLevel = r.success ? "INFO" : "ERROR";
        addLog(logLevel, "task", `[${token.name}] ${r.cmd}: ${r.success ? "OK" : r.error}`, { taskId: task.id });
        await logToDb(task.id, token.name, r.cmd, r.success ? "success" : "error", r.success ? undefined : r.error);

        // 保存角色信息到 Supabase（如果需要）
        if (r.success && r.data?.role) {
          // 可以在这里更新角色信息
        }
      }

      addLog("INFO", "task", `角色 ${token.name} 任务完成`, {
        taskId: task.id,
        successCount: cmdResults.filter((r) => r.success).length,
        failCount: cmdResults.filter((r) => !r.success).length,
      });
    } catch (err) {
      addLog("ERROR", "task", `执行失败 (${token.name}): ${err.message}`, { taskId: task.id });
      await logToDb(task.id, token.name, "batch", "error", err.message);
    } finally {
      client.disconnect();
    }

    // 角色之间间隔（使用 settings 中的 taskDelay，默认 2000）
    await sleep(bs.taskDelay || 2000);
  }

  addLog("INFO", "task", `任务完成: ${task.name}`, { taskId: task.id });
  processQueue();
}

// ==================== 记录日志到 Supabase ====================
async function logToDb(taskId, tokenName, taskType, status, message) {
  try {
    await supabase.from("task_logs").insert({
      task_id: taskId,
      token_name: tokenName,
      task_type: taskType,
      status,
      message: message || null,
    });
  } catch (e) {
    console.error("写入日志失败:", e.message);
  }
}

// ==================== 维护窗口（北京时间） ====================
// 周六 19:45 - 20:15 和 周日 19:45 - 21:35 禁止所有任务执行
const MAINTENANCE_WINDOWS = [
  { dayOfWeek: 6, startHour: 19, startMin: 45, endHour: 20, endMin: 15 }, // 周六
  { dayOfWeek: 0, startHour: 19, startMin: 45, endHour: 21, endMin: 35 }, // 周日
];

function isInMaintenanceWindow(beijingNow) {
  const day = beijingNow.getDay(); // 0=周日, 6=周六
  const hour = beijingNow.getHours();
  const min = beijingNow.getMinutes();

  for (const win of MAINTENANCE_WINDOWS) {
    if (win.dayOfWeek !== day) continue;
    const startMin = win.startHour * 60 + win.startMin;
    const endMin = win.endHour * 60 + win.endMin;
    const nowMin = hour * 60 + min;
    if (nowMin >= startMin && nowMin < endMin) {
      return true;
    }
  }
  return false;
}

function getBeijingTime() {
  const now = new Date();
  // 北京时间 = UTC+8
  return new Date(now.getTime() + 8 * 60 * 60 * 1000);
}

// ==================== 任务执行队列 ====================
// pendingQueue: 待执行的队列（等待上一个任务完成）
const pendingQueue = [];
// runningTasks: 正在执行的任务 ID 集合
const runningTasks = new Set();

async function enqueueTask(task) {
  // 检查是否已在运行
  if (runningTasks.has(task.id)) {
    addLog("WARN", "queue", `任务 ${task.name} 正在执行中，跳过此次触发`, { taskId: task.id });
    return;
  }

  // 检查维护窗口
  const bt = getBeijingTime();
  if (isInMaintenanceWindow(bt)) {
    addLog("WARN", "queue", `任务 ${task.name} 在维护窗口期，跳过: ${bt.toISOString().replace("T", " ").slice(0, 16)}`, { taskId: task.id });
    return;
  }

  // 如果没有任务在运行，立即执行
  if (runningTasks.size === 0) {
    runningTasks.add(task.id);
    await executeTask(task);
    runningTasks.delete(task.id);
    processQueue();
  } else {
    // 加入等待队列，等待60秒后执行
    addLog("INFO", "queue", `任务 ${task.name} 加入等待队列（当前有 ${runningTasks.size} 个任务在执行）`, { taskId: task.id });
    pendingQueue.push({
      task,
      scheduledAt: Date.now(),
      executeAt: Date.now() + 60 * 1000, // 60秒后
    });
  }
}

function processQueue() {
  // 检查等待队列
  if (pendingQueue.length === 0) return;
  const now = Date.now();

  // 找到已到执行时间的任务
  const ready = pendingQueue.filter((item) => item.executeAt <= now);
  if (ready.length > 0 && runningTasks.size === 0) {
    const item = ready[0];
    // 从队列移除
    const idx = pendingQueue.findIndex((i) => i === item);
    if (idx !== -1) pendingQueue.splice(idx, 1);

    addLog("INFO", "queue", `等待任务 ${item.task.name} 开始执行（已等待 ${Math.round((now - item.scheduledAt) / 1000)}s）`, { taskId: item.task.id });
    runningTasks.add(item.task.id);
    executeTask(item.task)
      .catch((err) => addLog("ERROR", "queue", `等待任务执行异常: ${item.task.name}`, { error: err.message }))
      .finally(() => {
        runningTasks.delete(item.task.id);
        processQueue();
      });
  }
}

// ==================== 定时任务调度 ====================
const cronJobs = new Map();
const taskSnapshot = new Map(); // 缓存任务快照，避免闭包引用过期

async function registerAllCrons() {
  // 停止旧的
  for (const job of cronJobs.values()) job.stop();
  cronJobs.clear();

  const { data: tasks } = await supabase
    .from("cron_tasks")
    .select("*")
    .eq("enabled", true);

  if (!tasks) return;

  // 更新快照
  for (const task of tasks) {
    taskSnapshot.set(task.id, task);
  }

  for (const task of tasks) {
    registerCron(task);
  }

  addLog("INFO", "cron", `已注册 ${tasks.length} 个定时任务`);
}

function registerCron(task) {
  if (cronJobs.has(task.id)) {
    cronJobs.get(task.id).stop();
  }
  if (!task.enabled || !task.cron_expr) return;

  try {
    // 使用北京时间时区，cron 表达式按北京时间解析
    const job = cron.schedule(task.cron_expr, () => {
      // 每次触发时从快照获取最新任务数据（确保 enabled/cron_expr 是最新的）
      const freshTask = taskSnapshot.get(task.id);
      if (!freshTask) return;
      enqueueTask(freshTask);
    }, {
      timezone: "Asia/Shanghai"
    });
    cronJobs.set(task.id, job);
    taskSnapshot.set(task.id, task);
    addLog("INFO", "cron", `注册定时任务: ${task.name} (${task.cron_expr})`);
  } catch (err) {
    addLog("ERROR", "cron", `注册失败: ${task.name}`, { error: err.message });
  }
}

// 启动时注册所有 cron
async function init() {
  addLog("INFO", "server", "服务器启动中...");
  addLog("INFO", "server", `Supabase: ${process.env.SUPABASE_URL}`);

  await registerAllCrons();

  // 每5分钟重新加载 cron（确保 Supabase 配置变更能生效）
  setInterval(async () => {
    addLog("INFO", "cron", "重新加载定时任务配置...");
    await registerAllCrons();
  }, 5 * 60 * 1000);

  addLog("INFO", "server", "服务器就绪");
}

// ==================== API 路由 ====================

// 健康检查
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    time: new Date().toISOString(),
    beijingTime: getBeijingTime().toISOString().replace("T", " ").slice(0, 19),
    activeCrons: cronJobs.size,
    runningTasks: [...runningTasks],
    queueLength: pendingQueue.length,
    logsInMemory: logs.length,
    maintenanceWindow: isInMaintenanceWindow(getBeijingTime()),
  });
});

// 获取 tokens
app.get("/api/tokens", async (req, res) => {
  const { data, error } = await supabase.from("tokens").select("*").order("created_at");
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// 添加 token
app.post("/api/tokens", async (req, res) => {
  const { data, error } = await supabase.from("tokens").insert(req.body).select();
  if (error) return res.status(400).json({ error: error.message });
  res.json(data);
});

// 更新 token
app.patch("/api/tokens/:id", async (req, res) => {
  const { data, error } = await supabase.from("tokens").update(req.body).eq("id", req.params.id).select();
  if (error) return res.status(400).json({ error: error.message });
  res.json(data);
});

// 删除 token
app.delete("/api/tokens/:id", async (req, res) => {
  const { error } = await supabase.from("tokens").delete().eq("id", req.params.id);
  if (error) return res.status(400).json({ error: error.message });
  res.json({ ok: true });
});

// 获取任务列表
app.get("/api/tasks", async (req, res) => {
  const { data, error } = await supabase.from("cron_tasks").select("*").order("created_at");
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// 创建定时任务
app.post("/api/tasks", async (req, res) => {
  const { data, error } = await supabase.from("cron_tasks").insert(req.body).select();
  if (error) return res.status(400).json({ error: error.message });
  // 立即注册
  if (data?.[0]) registerCron(data[0]);
  res.json(data);
});

// 更新任务（启停/编辑）
app.patch("/api/tasks/:id", async (req, res) => {
  const { data, error } = await supabase.from("cron_tasks").update(req.body).eq("id", req.params.id).select();
  if (error) return res.status(400).json({ error: error.message });
  // 重新注册
  await registerAllCrons();
  res.json(data);
});

// 删除任务
app.delete("/api/tasks/:id", async (req, res) => {
  const { error } = await supabase.from("cron_tasks").delete().eq("id", req.params.id);
  if (error) return res.status(400).json({ error: error.message });
  // 重新注册
  await registerAllCrons();
  res.json({ ok: true });
});

// 手动触发任务
app.post("/api/tasks/:id/run", async (req, res) => {
  const { data: task } = await supabase.from("cron_tasks").select("*").eq("id", req.params.id).single();
  if (!task) return res.status(404).json({ error: "任务不存在" });

  // 异步执行，走队列管理
  enqueueTask(task);

  res.json({ status: "running", task: task.name });
});

// 获取日志
app.get("/api/logs", (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 100, LOG_MAX);
  res.json(logs.slice(0, limit));
});

// 获取 DB 日志
app.get("/api/logs/db", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 100, 500);
  const { data, error } = await supabase
    .from("task_logs")
    .select("*")
    .order("created_at", { ascending: false })
    .limit(limit);
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// 获取任务定义列表（前端用）
app.get("/api/task-definitions", (req, res) => {
  const list = Object.entries(TASK_DEFINITIONS).map(([key, val]) => ({
    key,
    name: val.name,
    commands: val.commands.map((c) => c.cmd),
  }));
  res.json(list);
});

// ==================== 模板 API ====================
// 默认模板 ID（固定）
const DEFAULT_TEMPLATE_ID = "default_template";
const DEFAULT_TEMPLATE_SETTINGS = {
  arenaFormation: 1,
  towerFormation: 1,
  bossFormation: 1,
  bossTimes: 2,
  claimBottle: true,
  payRecruit: true,
  openBox: true,
  arenaEnable: true,
  claimHangUp: true,
  claimEmail: true,
  blackMarketPurchase: true
};

// GET /api/templates：自动确保有默认模板，列表带上 isDefault 标记
app.get("/api/templates", async (req, res) => {
  // 检查是否存在默认模板，不存在则自动创建
  const { data: existing } = await supabase
    .from("task_templates")
    .select("id")
    .eq("id", DEFAULT_TEMPLATE_ID)
    .single();
  if (!existing) {
    await supabase.from("task_templates").upsert({
      id: DEFAULT_TEMPLATE_ID,
      name: "默认",
      settings: DEFAULT_TEMPLATE_SETTINGS
    });
  }
  const { data, error } = await supabase.from("task_templates").select("*").order("created_at");
  if (error) return res.status(500).json({ error: error.message });
  // 标记默认模板
  const result = data.map(t => ({ ...t, isDefault: t.id === DEFAULT_TEMPLATE_ID }));
  res.json(result);
});

app.post("/api/templates", async (req, res) => {
  const { id, name, settings } = req.body;
  if (!name) return res.status(400).json({ error: "模板名称不能为空" });
  const { data, error } = await supabase
    .from("task_templates")
    .upsert({ id: id || Date.now().toString(), name, settings: settings || {} }, { onConflict: "id" })
    .select();
  if (error) return res.status(400).json({ error: error.message });
  res.json(data);
});

app.patch("/api/templates/:id", async (req, res) => {
  const { name, settings } = req.body;
  const updates = { updated_at: new Date().toISOString() };
  if (name !== undefined) updates.name = name;
  if (settings !== undefined) updates.settings = settings;
  const { data, error } = await supabase
    .from("task_templates")
    .update(updates)
    .eq("id", req.params.id)
    .select();
  if (error) return res.status(400).json({ error: error.message });
  res.json(data);
});

app.delete("/api/templates/:id", async (req, res) => {
  if (req.params.id === DEFAULT_TEMPLATE_ID) {
    return res.status(403).json({ error: "默认模板不可删除" });
  }
  const { error } = await supabase.from("task_templates").delete().eq("id", req.params.id);
  if (error) return res.status(400).json({ error: error.message });
  res.json({ ok: true });
});

// ==================== 全局设置 API ====================
// 注意：/api/settings/merged 必须在 :key 之前
app.get("/api/settings/merged", async (req, res) => {
  try {
    const { data: bsData } = await supabase
      .from("app_settings")
      .select("*")
      .eq("key", "batch_settings")
      .single();
    const batchSettings = bsData?.value || {};
    res.json({ batchSettings });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/settings/:key", async (req, res) => {
  const { data, error } = await supabase
    .from("app_settings")
    .select("*")
    .eq("key", req.params.key)
    .single();
  if (error && error.code === "PGRST116") {
    // 不存在则返回空默认值
    return res.json({ key: req.params.key, value: {} });
  }
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/api/settings/:key", async (req, res) => {
  const { value } = req.body;
  const { data, error } = await supabase
    .from("app_settings")
    .upsert(
      { key: req.params.key, value: value || {}, updated_at: new Date().toISOString() },
      { onConflict: "key" }
    )
    .select();
  if (error) return res.status(400).json({ error: error.message });
  res.json(data);
});

// 测试连接单个 token（调试用）
app.post("/api/test-token", async (req, res) => {
  const { tokenId } = req.body;
  if (!tokenId) return res.status(400).json({ error: "缺少 tokenId" });

  const { data: token } = await supabase.from("tokens").select("*").eq("id", tokenId).single();
  if (!token) return res.status(404).json({ error: "Token不存在" });

  let tokenData;
  try {
    tokenData = typeof token.token === "string" && token.token.startsWith("{")
      ? JSON.parse(token.token)
      : { roleToken: token.token, roleId: token.roleId, sessId: token.sessId, connId: token.connId, isRestore: token.isRestore };
  } catch (err) {
    return res.status(400).json({ error: `Token解析失败: ${err.message}` });
  }

  tokenData.name = token.name;
  const client = new GameClient(tokenData, token.ws_url);

  try {
    await client.connect(15000);
    const roleInfo = await client.sendWithPromise("role_getroleinfo", {}, 8000);
    const results = await client.executeBatch([
      { cmd: "system_signinreward", params: {} },
      { cmd: "system_claimhangupreward", params: {} },
      { cmd: "task_claimdailyreward", params: { rewardId: 0 } },
    ], 800);
    client.disconnect();

    res.json({
      success: true,
      roleInfo,
      testResults: results,
    });
  } catch (err) {
    client.disconnect();
    res.status(500).json({ error: err.message });
  }
});

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ==================== 启动 ====================
const PORT = process.env.PORT || 3000;
init().then(() => {
  app.listen(PORT, () => {
    addLog("INFO", "server", `监听端口 ${PORT}`);
  });
});
