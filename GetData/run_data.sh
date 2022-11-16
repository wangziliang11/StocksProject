source /Users/user/opt/anaconda3/etc/profile.d/conda.sh
conda activate StocksProject
python getStaticData.py
for str in 'wfq' 'qfq' 'hfq'
do
python getGeneralData_muti.py --adj=$str --threads=10
sleep 30
done

python getDailyBasic.py

#for str in 'wfq' 'qfq' 'hfq'
#do
#python getGeneralData_muti.py --freq='M' --adj=$str --threads=10
#sleep 30
#python getGeneralData_muti.py --freq='W' --adj=$str --threads=10
#sleep 30
#done


