library(biomaRt) # biomaRt_2.30.0

snp_mart = useMart("ENSEMBL_MART_SNP", dataset="hsapiens_snp")
snps_table = read.table("/Users/bruno.ariano/work/HT/tiledb/data/hapmap3_SNPs/eur_w_ld_chr/w_hm3.snplist", h = T)
snp_ids = snps_table$SNP
snp_attributes = c('refsnp_id', 'chr_name', 'chrom_start', 'chrom_end', 'allele')

snp_locations = getBM(attributes=snp_attributes, filters="snp_filter",
                      values=snp_ids, mart=snp_mart)
write.table(snp_locations,"/Users/bruno.ariano/work/HT/tiledb/data/hapmap3_SNPs/rsid_mapped.txt",
            col.names = T, row.names = F, quote = F)
