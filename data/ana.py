max=0
with open('/home/markus/raid/cnf/sat2006/SAT-Race-Benchmarks/velev-npe-1.0-9dlx-b71.cnf', 'rU') as f:
  for line in f:
    l = len(line.split(" "))
    if (l > max):
      max = l
print max
