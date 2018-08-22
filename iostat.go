package main

import (
	"os/exec"
	"os"
	"encoding/json"
	"fmt"
	"sort"
	"time"
	"flag"
	"strconv"
)


type PoolInfoLuminous struct {
	Name string
	Id uint
	SizeBytes uint `json:"size_bytes"`
	SizeKb uint `json:"size_kb"`
	NumObjects uint `json:"num_objects"`
	NumObjectClones uint `json:"num_object_clones"`
	NumObjectCopies uint `json:"num_object_copies"`
	NumObjectMissingOnPrimary uint `json:"num_objects_missing_on_primary"`
	NumObjectUnfound uint `json:"num_objects_unfound"`
	NumObjectDegraded uint `json:"num_objects_degraded"`
	ReadOps uint `json:"read_ops"`
	ReadBytes uint `json:"read_bytes"`
	WriteOps uint `json:"write_ops"`
	WriteBytes uint `json:"write_bytes"`
};

type RadosDFLuminous struct {
	TotalObjects uint `json:"total_objects"`
	TotalUsed uint `json:"total_used"`
	TotalAvail uint `json:"total_avail"`
	TotalSpace uint `json:"total_space"`
	Pools []PoolInfoLuminous
};

func main() {
	lineFormat := "%30s    %10d  %10d  %10d  %10d  %10d  %10d\n"
	lineFormatHdr := "%30s    %10s  %10s  %10s  %10s  %10s  %10s\n"

	// timeoutPtr := flag.Int("timeout", 5, "timeout between measurements (better to be 5s+)")
	flag.Parse()

	var radosdf1, radosdf2 RadosDFLuminous
	radosdf_curr := &radosdf1
	var radosdf_prev, redosdf_tmp *RadosDFLuminous

	var timeout_s uint
	if (len(flag.Args()) == 1) {
		timeout_s_t, err := strconv.Atoi(flag.Args()[0])
		if err != nil {
			fmt.Println("Failed to parse timeout option", err)
			return
		}
		timeout_s = uint(timeout_s_t)
	} else {
		timeout_s = 5;
	}
	
	timeout_ns := time.Duration(timeout_s) * time.Second
	for cnt := -1 ;; cnt++ {
		start := time.Now()
		output, err := exec.Command("rados", "df", "-f", "json").CombinedOutput()
		if err != nil {
			os.Stderr.WriteString(err.Error())
			break
		}

		err = json.Unmarshal([]byte(output), radosdf_curr)
		if err != nil {
			fmt.Println("error:", err)
			break
		}

		if (radosdf_prev != nil) {

			fmt.Printf(lineFormatHdr, "name", "wr. IOPS", "wr. MiBps", "rd. IOPS", "rd. MiBps", "+objs/s", "+MiB/s")

			pools_map := make(map[string]PoolInfoLuminous)
			for _, pinfo := range radosdf_prev.Pools {
				pools_map[pinfo.Name] = pinfo
			}

			sort.Slice(radosdf_curr.Pools, func(i, j int) bool {
				return radosdf_curr.Pools[i].Name < radosdf_curr.Pools[j].Name
			})

			for _, pinfo := range radosdf_curr.Pools {
				if ppinfo, ok := pools_map[pinfo.Name]; ok {
					fmt.Printf(lineFormat, pinfo.Name,
						(pinfo.WriteOps - ppinfo.WriteOps) / timeout_s,
						(pinfo.WriteBytes - ppinfo.WriteBytes) / 1024 / 1024 / timeout_s,
						(pinfo.ReadOps - ppinfo.ReadOps) / timeout_s,
						(pinfo.ReadBytes - ppinfo.ReadBytes) / 1024 / 1024 / timeout_s,
						(pinfo.NumObjects - ppinfo.NumObjects) / timeout_s,
						(pinfo.SizeBytes - ppinfo.SizeBytes) / 1024 / 1024 / timeout_s)
				}
			}

			fmt.Println()

			redosdf_tmp = radosdf_prev
			radosdf_prev = radosdf_curr
			radosdf_curr = redosdf_tmp

		} else {
			radosdf_prev = radosdf_curr
			radosdf_curr = &radosdf2
		}

		elapsed := time.Since(start)
		if (elapsed < timeout_ns) {
			time.Sleep(timeout_ns - elapsed)
		}
	}
}
