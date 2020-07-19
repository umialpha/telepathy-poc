mkdir -p ./jobs
for i in 1 
do
	cat bench.yaml.tpl | sed "s/\$ITEM/$i/" > ./jobs/bench-$i.yaml
done

