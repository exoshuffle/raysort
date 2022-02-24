declare -a splits=(2000000000 500000000 100000000)
declare -a modes=("--simple_shuffle" "--manual_spilling" "" "--riffle" "--magnet")
TOTAL_GB=512

for split in "${splits[@]}"
do
    for mode in "${modes[@]}"
    do
        ~/raysort/ansible/start_ray.sh
        sleep 10
        python ~/raysort/raysort/main.py --total_gb=$TOTAL_GB --input_part_size=$split $mode
        sleep 10
    done
done
# ~/raysort/ansible/start_ray.sh
# sleep 10
# python ~/raysort/raysort/main.py --total_gb=512
# sleep 10

# ~/raysortansible/start_ray.sh
# sleep 10
# python ~/raysort/raysort/main.py --total_gb=512 --riffle
# sleep 10

# ~/raysort/ansible/start_ray.sh
# sleep 10
# python ~/raysort/raysort/main.py --total_gb=512 --simple_shuffle
# sleep 10

# ~/raysort/ansible/start_ray.sh
# sleep 10
# python ~/raysort/raysort/main.py --total_gb=512 --simple_shuffle
# sleep 10
