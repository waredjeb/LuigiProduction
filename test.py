import numpy as np

nevents = 200000
ntriggers = 2
eff_init_data = [0.4, 0.6, 0.2]
eff_init_mc   = [0.65, 0.35, 0.15]
assert len(eff_init_data)==ntriggers+1
assert len(eff_init_mc)==ntriggers+1
# 0: trigger 1
# 1: trigger 2
# 2: trigger 1 AND trigger 2
choices = [0,1]

trigger_decisions_data = np.random.choice(choices, size=nevents, p=eff_init_data[:-1])
trigger_decisions_mc = np.random.choice(choices, size=nevents, p=eff_init_mc[:-1])
print(trigger_decisions_data)
print(trigger_decisions_mc)
occur_data = np.array([ np.count_nonzero( (trigger_decisions_data==ch)) for ch in choices ])
occur_mc = np.array([ np.count_nonzero( (trigger_decisions_mc==ch)) for ch in choices ])
eff_data = occur_data / nevents
eff_mc = occur_mc / nevents
print(eff_data)
print(eff_mc)

mask_data = np.random.choice(choices,
                             size=trigger_decisions_data.shape,
                             p=[1-eff_init_data[-1],eff_init_data[-1]]).astype(bool)
mask_mc = np.random.choice(choices,
                           size=trigger_decisions_mc.shape,
                           p=[1-eff_init_mc[-1],eff_init_mc[-1]]).astype(bool)
print(mask_data)
print(mask_mc)
trigger_decisions_data[mask_data] = 2
trigger_decisions_mc[mask_mc] = 2
print(trigger_decisions_data)
print(trigger_decisions_mc)

occur_data = np.array([ np.count_nonzero( np.where((trigger_decisions_data==ch))) for ch in choices ])
occur_mc = np.array([ np.count_nonzero( np.where((trigger_decisions_mc==ch))) for ch in choices ])
eff_data = occur_data / nevents
eff_mc = occur_mc / nevents
print(eff_data)
print(eff_mc)
