 
ifsname="eth0 eth1 eth2 br0"

for i in $ifsname
do
	echo "Searching at interface $i"

	data=$(ip link show dev $i 2> /dev/null | grep 'state UP') || continue
	data=$(ip addr show dev $i 2> /dev/null | grep 'inet') || continue
	data=$(echo $data | head -n 1 | awk '{print $2}' | sed s:/.*::)
	
	break
done

echo -e "\nFound $data for interface $i"

sed -e s:%ADDRESS%:$data: -e s:%HOSTNAME%:$HOSTNAME: manp2p.conf 