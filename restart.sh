docker compose down --remove-orphans
rm -rf T-DAT
git clone https://github.com/Izzoudine/T-DAT.git
cd T-DAT/kafka
chmod +x startup.sh
./startup.sh