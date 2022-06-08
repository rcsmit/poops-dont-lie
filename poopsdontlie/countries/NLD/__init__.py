from poopsdontlie.countries.NLD.regions import rna_flow_per_100k_people_for_rwzi, \
    rna_flow_per_capita_for_gemeente, rna_flow_per_capita_for_veiligheidsregio, rna_flow_per_capita_for_rwzi, \
    smoothed_rna_flow_per_capita_for_rwzi, smoothed_rna_flow_per_capita_for_veiligheidsregio, smoothed_rna_flow_per_capita_for_gemeente, \
    smoothed_rna_flow_per_capita_national_level

regions = {
    ('rivm_sewage_treatment_plant', 'rivm_rwzi'): ('Original RIVM dataset on RNA Flow per ML normalized to 1-in-100k people per sewage treatment plant', rna_flow_per_100k_people_for_rwzi),
    ('sewage_treatment_plant', 'rwzi'): ('RNA Flow per ML normalized per capita per sewage treatment plant', rna_flow_per_capita_for_rwzi),
    ('smooth_sewage_treatment_plant', 'smooth_rwzi'): ('Smoothed with 95% CI dataset on RNA Flow per ML normalized per capita per sewage treatment plant', smoothed_rna_flow_per_capita_for_rwzi),
    ('municipality', 'gemeente'): ('RNA Flow per ML of sewage normalized per capita per municipality', rna_flow_per_capita_for_gemeente),
    ('smooth_municipality', 'smooth_gemeente'): ('Smoothed with 95% CI RNA Flow per ML of sewage normalized per capita per municipality', smoothed_rna_flow_per_capita_for_gemeente),
    ('safety_region', 'veiligheidsregio'): ('RNA Flow per ML of sewage normalized per capita per safety region', rna_flow_per_capita_for_veiligheidsregio),
    ('smooth_safety_region', 'smooth_veiligheidsregio'): ('Smoothed with 95% CI RNA Flow per ML of sewage normalized per capita per safety region', smoothed_rna_flow_per_capita_for_veiligheidsregio),
    #('rwzi_count_per_municipality', 'rwzi_aantal_per_gemeente'): ('The number of sewage treatment plants (partially) contributing to a municipality per date', ),
    #('rwzi_count_per_safety_region', 'rwzi_aantal_per_veiligheidsregio'): ('The number of sewage treatment plants (partially) contributing to a safety region per date', ),
    ('smooth_national_level', 'smooth_nationaal_niveau'): ('Smoothed with 95% CI on median model fit RNA Flow per ML of sewage normalized per capita on a national level', smoothed_rna_flow_per_capita_national_level)
}
