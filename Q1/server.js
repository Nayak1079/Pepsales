const express = require("express");
const axios = require("axios");
const redis = require("redis");
const cron = require("node-cron");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3000;

// API Configuration
const API_BASE_URL = "http://20.244.56.144/evaluation-service";
const API_KEY = process.env.API_KEY; // Secure API Key from .env
const REDIS_PORT = 6379;

// Redis Client Setup
const redisClient = redis.createClient({ socket: { host: "127.0.0.1", port: REDIS_PORT } });

redisClient
  .connect()
  .then(() => console.log("Redis connected"))
  .catch((err) => console.error("Redis connection failed:", err));

// Debugging API Key Load
console.log(API_KEY ? "API Key loaded successfully" : "API Key missing!");

// Fetch Users & Cache in Redis
async function fetchUsers() {
  try {
    console.log("Fetching users...");
    const { data } = await axios.get(`${API_BASE_URL}/users`, {
      headers: { Authorization: `Bearer ${API_KEY}` },
    });

    await redisClient.set("users", JSON.stringify(data.users), { EX: 3600 });
    console.log("Users cached in Redis");
    return data.users;
  } catch (error) {
    console.error("Failed to fetch users:", error.message);
    return {};
  }
}

// Fetch Posts & Cache in Redis
async function fetchPosts() {
  try {
    console.log("Fetching posts...");
    const users = JSON.parse((await redisClient.get("users")) || "{}");
    let allPosts = [];

    for (const userId in users) {
      try {
        const { data } = await axios.get(`${API_BASE_URL}/users/${userId}/posts`, {
          headers: { Authorization: `Bearer ${API_KEY}` },
        });
        allPosts.push(...data.posts);
        console.log(`Retrieved ${data.posts.length} posts for user ${userId}`);
      } catch (err) {
        console.error(`Failed to get posts for user ${userId}:`, err.message);
      }
    }

    await redisClient.set("posts", JSON.stringify(allPosts), { EX: 3600 });
    console.log("Posts cached in Redis");
    return allPosts;
  } catch (error) {
    console.error("Error fetching posts:", error.message);
    return [];
  }
}

// Fetch Comments & Cache in Redis
async function fetchComments() {
  try {
    console.log("Fetching comments...");
    const posts = JSON.parse((await redisClient.get("posts")) || "[]");
    let commentCounts = {};

    for (const post of posts) {
      try {
        const { data } = await axios.get(`${API_BASE_URL}/posts/${post.id}/comments`, {
          headers: { Authorization: `Bearer ${API_KEY}` },
        });
        commentCounts[post.id] = data.comments.length;
      } catch (err) {
        console.error(`Failed to fetch comments for post ${post.id}:`, err.message);
      }
    }

    await redisClient.set("comments", JSON.stringify(commentCounts), { EX: 3600 });
    console.log("Comments cached in Redis");
    return commentCounts;
  } catch (error) {
    console.error("Error fetching comments:", error.message);
    return {};
  }
}

// API Routes
app.get("/", (req, res) => {
  res.json({ message: "Welcome to the Social Media Analytics Microservice!" });
});

// Refresh Users
app.get("/refresh-users", async (req, res) => {
  const users = await fetchUsers();
  res.json({ message: "Users refreshed successfully", users });
});

// Refresh Posts
app.get("/refresh-posts", async (req, res) => {
  const posts = await fetchPosts();
  res.json({ message: "Posts refreshed successfully", posts });
});

// Refresh Comments
app.get("/refresh-comments", async (req, res) => {
  const comments = await fetchComments();
  res.json({ message: "Comments refreshed successfully", comments });
});

// Get Top 5 Users with Most Posts
app.get("/users", async (req, res) => {
  const posts = JSON.parse((await redisClient.get("posts")) || "[]");
  let postCounts = {};

  posts.forEach((post) => {
    postCounts[post.userid] = (postCounts[post.userid] || 0) + 1;
  });

  const users = JSON.parse((await redisClient.get("users")) || "{}");
  const topUsers = Object.entries(postCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([id, count]) => ({ id, name: users[id], post_count: count }));

  res.json(topUsers);
});

// Get Latest or Most Popular Posts
app.get("/posts", async (req, res) => {
  const { type } = req.query;
  const posts = JSON.parse((await redisClient.get("posts")) || "[]");

  if (type === "latest") {
    return res.json(posts.sort((a, b) => b.id - a.id).slice(0, 5));
  }

  if (type === "popular") {
    const comments = JSON.parse((await redisClient.get("comments")) || "{}");
    const maxComments = Math.max(...Object.values(comments));
    const popularPosts = posts.filter((post) => comments[post.id] === maxComments);

    return res.json(popularPosts);
  }

  res.status(400).json({ message: "Invalid type parameter. Use 'latest' or 'popular'." });
});

// Schedule Data Refresh Every Hour
cron.schedule("0 * * * *", async () => {
  console.log("Running scheduled data refresh...");
  await fetchUsers();
  await fetchPosts();
  await fetchComments();
  console.log("Data refresh complete.");
});

// Start Server
app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
