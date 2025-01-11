clear

// Load firm data - gvkey--year
import delimited "full_data.csv" 

*adjust dates
gen date2 = quarterly(datacqtr, "YQ")
format date2 %tq
gen quarter_only = quarter(date2)

*TOBINS Q : (AT + MV - BV ) / AT // ***this one is better***
gen tobins_q = (atq + mv1 - ceqq ) / atq
winsor2 tobins_q, replace cuts(1 99)
by gvkey: gen tobins_q_t4 = tobins_q[_n-4] // 
by gvkey: gen tobins_q_t1 = tobins_q[_n-1] // 
by gvkey: gen tobins_q_chng_qtr =(tobins_q - tobins_q[_n-1] )
by gvkey: gen tobins_q_chng_yr =(tobins_q - tobins_q[_n-4] )
gen tobins_q_log = log(tobins_q)
by gvkey: gen tobins_q_log_t4 = tobins_q_log[_n-4] // 

//Sales
by gvkey: gen saleqwbyatq_t4 = saleqwbyatq[_n-4] // 

*generate log variables
foreach i in saleqwbyatq saleqwbyatq_t4 { 
sort gvkey datacqtr
				by gvkey: gen log_`i' =log(`i')
}

gen lsize = log(mv1)
gen lroa = log(roa)
gen lbmr = log(bmr)

winsor2 lroa, replace cuts(1 99)
winsor2 lbmrw, replace cuts(1 99)
winsor2 lsize, replace cuts(1 99)

// Purchuases
by gvkey : gen pur = (invtq - invtq[_n-1] + cogsq ) // 
gen pur_by_atq = pur / atq // 
winsor2 pur_by_atq, replace cuts(0.05 99.5)
by gvkey: gen pur_by_atq_t4 = pur_by_atq[_n-4]
by gvkey: gen purg_past =(pur - pur[_n-4] )/pur[_n-4] // 
by gvkey: gen purg_past_atq =(pur - pur[_n-4] )/atq // 

//* altamn - z
gen alt_z = 1.2 *(wcapq /atq) + 1.4 *(req/atq) + 3.3 *(ibq/atq) + 0.6* (mv1 / ltq) + saleq / atq
winsor2 alt_z, replace cuts(1 99)

*adjust naics
tostring naics, gen(naics2)
gen naicstwo = real(substr(naics2,1,2))
gen naicsone = real(substr(naics2,1,1))

*Adjust data set
drop if saleqw < 10
drop if invtqw <= 1
drop if invtqw == .
drop if cogsqw <= 1
drop if cogsqw == .
		
//  growth
foreach i in  saleqw invtqw  { 
	sort gvkey datacqtr
		by gvkey: gen `i'g_past = (`i' - `i'[_n-4] )/`i'[_n-4]  
		by gvkey: gen `i'g_past_log =log(`i' /`i'[_n-4])
}

*gen variables for scom
gen scome = 0
replace scome = scm_flag + coo_flag
replace scome = 1 if  scome > 0

//*** generate variables for past SCOM***///
*gen scom in times of crisis
gen scom_10th_disruption = scome * high_epu_mean_diff_10th_indicato
// lag 5 quarters
	by gvkey: gen scome_t5 = scome[_n-5] 
	foreach i in scome {
	by gvkey: gen scom_10th_disruption_t1_v2 = `i'[_n-1]* high_epu_mean_diff_10th_indicato 		// lag 1 quarters
	}

// Cash Convergace Cycle //𝐶𝐶𝐶=365∗(𝐴𝑣𝑔.𝐼𝑛𝑣𝑒𝑛𝑡𝑜𝑟𝑖𝑒𝑠/𝐶𝑂𝐺𝑆 +𝐴𝑣𝑔.𝐴𝑐𝑐𝑜𝑢𝑛𝑡𝑠 𝑅𝑒𝑐𝑒𝑖𝑣𝑎𝑏𝑙𝑒𝑠/𝑆𝑎𝑙𝑒𝑠 −𝐴𝑣𝑔.𝐴𝑐𝑐𝑜𝑢𝑛𝑡𝑠 𝑃𝑎𝑦𝑎𝑏𝑙𝑒𝑠/𝐶𝑂𝐺𝑆 ).  
// DIO = 365 * 0.5 * (INVTQt + INVTQt-1) / COGSQt
// DRO = 365 * 0.5 * (RECTQt + RECTQt-1) / REVTQt
// DPO = 365 * 0.5 * (APQt + APQt-1) / COGSQt
by gvkey: gen dio = 365*0.5*(invtq+invtq[_n-1])/cogsq
by gvkey: gen dro = 365*0.5*(rectq+rectq[_n-1])/saleq
by gvkey: gen dpo = 365*0.5*(apq+apq[_n-1])/cogsq
// CCC = DIO + DRO - DPO
gen ccc = dio + dro - dpo
gen lnccc= ln(ccc)
by gvkey: gen ccc_t4 = ccc[_n-4]
by gvkey: gen lnccc_t4 = lnccc[_n-4]

// OperatingCycle = DIO + DRO
gen OperatingCycle = dio + dro
gen lnOperatingCycle = ln(OperatingCycle)
by gvkey: gen OperatingCycle_t4 = OperatingCycle[_n-4]
by gvkey: gen lnOperatingCycle_t4 = lnOperatingCycle[_n-4]

*Keep only manufacturing
keep if naicsone == 3


