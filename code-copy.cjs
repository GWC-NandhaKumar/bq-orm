/* eslint-disable no-undef */
/* eslint-disable @typescript-eslint/no-require-imports */
const fs = require("fs");
const path = require("path");

// === CONFIG ===
const ROOT_DIR = "./src"; // change this to your project root
const OUTPUT_FILE = "project_dump.txt";

const CODE_EXTENSIONS = new Set([
  ".js",
  ".ts",
  ".py",
  ".java",
  ".cpp",
  ".c",
  ".h",
  ".html",
  ".css",
  ".php",
  ".rb",
]);

function getFolderStructure(dir, depth = 0) {
  let result = "";
  const items = fs.readdirSync(dir, { withFileTypes: true });

  for (const item of items) {
    const indent = "    ".repeat(depth);
    if (item.isDirectory()) {
      result += `${indent}${item.name}/\n`;
      result += getFolderStructure(path.join(dir, item.name), depth + 1);
    } else {
      result += `${indent}${item.name}\n`;
    }
  }
  return result;
}

function getCodeFiles(dir) {
  let result = "";
  const items = fs.readdirSync(dir, { withFileTypes: true });

  for (const item of items) {
    const fullPath = path.join(dir, item.name);
    if (item.isDirectory()) {
      result += getCodeFiles(fullPath);
    } else {
      const ext = path.extname(item.name).toLowerCase();
      if (CODE_EXTENSIONS.has(ext)) {
        try {
          const content = fs.readFileSync(fullPath, "utf-8");
          result += `\n\n===== ${fullPath} =====\n\n${content}`;
        } catch (err) {
          result += `\n\n===== ${fullPath} =====\n\n[Could not read: ${err.message}]`;
        }
      }
    }
  }
  return result;
}

function main() {
  console.log("Generating project dump...");

  let output = "=== PROJECT FOLDER STRUCTURE ===\n\n";
  output += getFolderStructure(ROOT_DIR);

  output += "\n\n=== CODE FILES CONTENT ===\n";
  output += getCodeFiles(ROOT_DIR);

  fs.writeFileSync(OUTPUT_FILE, output, "utf-8");

  console.log(`✅ Project dump saved in ${OUTPUT_FILE}`);
}

main();
