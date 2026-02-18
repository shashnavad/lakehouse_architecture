#!/bin/bash
# Helper script to push to GitHub
# Usage: ./push_to_github.sh YOUR_GITHUB_USERNAME

if [ -z "$1" ]; then
    echo "Usage: ./push_to_github.sh YOUR_GITHUB_USERNAME"
    echo ""
    echo "Example: ./push_to_github.sh shashnavad"
    exit 1
fi

GITHUB_USERNAME=$1
REPO_NAME="lakehouse_architecture"

echo "🚀 Preparing to push to GitHub..."
echo ""

# Check if remote already exists
if git remote get-url origin &>/dev/null; then
    echo "⚠️  Remote 'origin' already exists:"
    git remote get-url origin
    echo ""
    read -p "Do you want to update it? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git remote set-url origin "https://github.com/${GITHUB_USERNAME}/${REPO_NAME}.git"
    else
        echo "Keeping existing remote."
    fi
else
    echo "➕ Adding remote origin..."
    git remote add origin "https://github.com/${GITHUB_USERNAME}/${REPO_NAME}.git"
fi

# Rename branch to main if needed
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "📝 Renaming branch from '${CURRENT_BRANCH}' to 'main'..."
    git branch -M main
fi

echo ""
echo "📋 Current status:"
git status --short

echo ""
echo "📦 Ready to push! Run the following command:"
echo ""
echo "   git push -u origin main"
echo ""
echo "Or if you prefer SSH:"
echo "   git remote set-url origin git@github.com:${GITHUB_USERNAME}/${REPO_NAME}.git"
echo "   git push -u origin main"
echo ""
echo "⚠️  Make sure you've created the repository on GitHub first!"
echo "   Visit: https://github.com/new"
echo "   Repository name: ${REPO_NAME}"
echo "   DO NOT initialize with README, .gitignore, or license"
echo ""

read -p "Do you want to push now? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🚀 Pushing to GitHub..."
    git push -u origin main
    if [ $? -eq 0 ]; then
        echo ""
        echo "✅ Successfully pushed to GitHub!"
        echo "   View your repo: https://github.com/${GITHUB_USERNAME}/${REPO_NAME}"
    else
        echo ""
        echo "❌ Push failed. Make sure:"
        echo "   1. The repository exists on GitHub"
        echo "   2. You have push permissions"
        echo "   3. You're authenticated (check: gh auth status)"
    fi
else
    echo "Push cancelled. Run 'git push -u origin main' when ready."
fi

