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
      // 连接
      await client.connect(15000);

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
        const taskDef = TASK_DEFINITIONS[tid];
        if (!taskDef) {
          addLog("WARN", "task", `未知任务: ${tid} (${token.name})`, { taskId: task.id });
          cmdResults.push({ cmd: tid, success: false, error: "未知任务" });
          continue;
        }
        const results = await client.executeBatch(taskDef.commands, 800);
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
      await logToDb(task.id, token.name, taskDef.name, "error", err.message);
    } finally {
      client.disconnect();
    }

    // 角色之间间隔 2 秒
    await sleep(2000);
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
    const job = cron.schedule(task.cron_expr, () => {
      // 每次触发时从快照获取最新任务数据（确保 enabled/cron_expr 是最新的）
      const freshTask = taskSnapshot.get(task.id);
      if (!freshTask) return;
      enqueueTask(freshTask);
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
