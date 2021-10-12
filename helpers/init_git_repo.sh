################################################################################
# Checkout the repo for a PR and add the remote of the target branch           #
################################################################################

# Fetches master branch from GitHub and "resets" local changes to be relative to it,
# so we can diff what changed relatively to master branch.

echo "initiated process.."
git init
git config user.email "devisahay.mishra@learningmate.com"
git config user.name "devisahay.mishra"

git commit -m "empty commit"
git remote add origin "${BASE_REPO_URL}"
git fetch origin master


# Fetch all PRs to get history for PRs created from forked repos
git fetch origin +refs/pull/*/merge:refs/remotes/origin/pr/*

git reset --hard "origin/pr/${PR_NUMBER}"

if ! git rebase "origin/${BASE_BRANCH}"
then
  exit 1
fi

echo "successfully rebased PR  #${PR_NUMBER} on master"
echo "Working properly."