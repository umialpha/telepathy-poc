mkdir -p ./jobs
for i in 1 2 3 4 5
do
	cat bench.yaml.tpl | sed "s/\$ITEM/$i/" > ./jobs/bench-$i.yaml
done

