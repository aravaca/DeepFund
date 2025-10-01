cd frontend
npm run build
cd ..
git add .
git commit -m "calc quota change"
git pull --rebase origin main
git push origin main