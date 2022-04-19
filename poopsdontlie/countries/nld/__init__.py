from poopsdontlie.countries.nld.regions import rna_flow_per_100k_people_for_rwzi, \
    rna_flow_per_100k_people_for_gemeente, rna_flow_per_100k_people_for_veiligheidsregio

regions = {
    ('sewage_treatment_plant', 'rwzi'): ('Original RIVM dataset on RNA Flow per ML normalized to 1-in-100k people per sewage treatment plant', rna_flow_per_100k_people_for_rwzi),
    ('municipality', 'gemeente'): ('RNA Flow per ML of sewage normalized to 1-in-100k people per municipality', rna_flow_per_100k_people_for_gemeente),
    ('safety_region', 'veiligheidsregio'): ('RNA Flow per ML of sewage normalized to 1-in-100k people per safety region', rna_flow_per_100k_people_for_veiligheidsregio),
    #('rwzi_count_per_municipality', 'rwzi_aantal_per_gemeente'): ('The number of sewage treatment plants (partially) contributing to a municipality per date', ),
    #('rwzi_count_per_safety_region', 'rwzi_aantal_per_veiligheidsregio'): ('The number of sewage treatment plants (partially) contributing to a safety region per date', ),
}
