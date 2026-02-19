#!/bin/bash
# Script to create GitHub repository and push

echo "🚀 Creating GitHub repository and pushing code..."
echo ""

# Check if GitHub CLI is installed
if ! command -v gh &> /dev/null; then
    echo "❌ GitHub CLI (gh) is not installed."
    echo "   Install it: brew install gh"
    echo "   Or create the repository manually at: https://github.com/new"
    exit 1
fi

# Check authentication
if ! gh auth status &> /dev/null; then
    echo "⚠️  Not authenticated with GitHub CLI"
    echo "   Authenticating..."
    gh auth login
fi

# Create repository
echo "📦 Creating repository on GitHub..."
gh repo create lakehouse_architecture \
    --public \
    --description "Modern data lakehouse architecture using Delta Lake - Medallion Architecture (Bronze → Silver → Gold)" \
    --source=. \
    --remote=origin \
    --push

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Successfully created and pushed to GitHub!"
    echo "   View your repo: https://github.com/shashnavad/lakehouse_architecture"
else
    echo ""
    echo "❌ Failed to create repository. Try creating it manually:"
    echo "   1. Go to: https://github.com/new"
    echo "   2. Repository name: lakehouse_architecture"
    echo "   3. DO NOT initialize with README, .gitignore, or license"
    echo "   4. Then run: git push -u origin main"
fi

