# Creation of datasets

## HapMap3

Following Sodbo instructions the data was downloaded from here: [https://zenodo.org/records/8182036](https://zenodo.org/records/8182036)

The file called w_hm3.snplist was processed using the Rscript convert_rsid.R within this folder. After Mapping the w_hm3.snplist data was mapped and sorted into the obtained list of SNPs from R using the following custom awk script:

```
awk 'NR==FNR{a[$1]= $2" "$3; next} { split(a[$1], arr, " "); if (arr[1] > arr[2]) { print $2 "," $3 "," arr[2] "," arr[1] } else { print $2 "," $3 "," arr[1] "," arr[2] } }' w_hm3.snplist rsid_mapped.txt > rsid_mapped_sorted.txt
```
