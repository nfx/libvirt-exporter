// Copyright 2017 Kumina, https://kumina.nl/
// Copyright 2019, Alexey Kostin
// Copyright 2026, Serge Smertin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Project forked from https://github.com/rumanzo/libvirt_exporter_improved

package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"flag"

	"github.com/digitalocean/go-libvirt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	libvirtUpDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "", "up"),
		"Whether scraping libvirt's metrics was successful.",
		nil,
		nil)

	libvirtDomainInfoMaxMemDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_info", "maximum_memory_bytes"),
		"Maximum allowed memory of the domain, in bytes.",
		[]string{"domain"},
		nil)
	libvirtDomainInfoMemoryUsageDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_info", "memory_usage_bytes"),
		"Memory usage of the domain, in bytes.",
		[]string{"domain"},
		nil)
	libvirtDomainInfoNrVirtCpuDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_info", "virtual_cpus"),
		"Number of virtual CPUs for the domain.",
		[]string{"domain"},
		nil)
	libvirtDomainInfoCpuTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_info", "cpu_time_seconds_total"),
		"Amount of CPU time used by the domain, in seconds.",
		[]string{"domain"},
		nil)
	libvirtDomainInfoVirDomainState = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_info", "vstate"),
		"Virtual domain state. 0: no state, 1: the domain is running, 2: the domain is blocked on resource,"+
			" 3: the domain is paused by user, 4: the domain is being shut down, 5: the domain is shut off,"+
			"6: the domain is crashed, 7: the domain is suspended by guest power management",
		[]string{"domain"},
		nil)

	libvirtDomainBlockRdBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "read_bytes_total"),
		"Number of bytes read from a block device, in bytes.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockRdReqDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "read_requests_total"),
		"Number of read requests from a block device.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockRdTotalTimesDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "read_time_total"),
		"Total time (ns) spent on reads from a block device, in ns, that is, 1/1,000,000,000 of a second, or 10−9 seconds.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockWrBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "write_bytes_total"),
		"Number of bytes written to a block device, in bytes.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockWrReqDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "write_requests_total"),
		"Number of write requests to a block device.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockWrTotalTimesDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "write_time_total"),
		"Total time (ns) spent on writes on a block device, in ns, that is, 1/1,000,000,000 of a second, or 10−9 seconds.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockFlushReqDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "flush_requests_total"),
		"Total flush requests from a block device.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockFlushTotalTimesDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "flush_total"),
		"Total time (ns) spent on cache flushing to a block device, in ns, that is, 1/1,000,000,000 of a second, or 10−9 seconds.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockAllocationDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "allocation"),
		"Offset of the highest written sector on a block device.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockCapacityDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "capacity"),
		"Logical size in bytes of the block device	backing image.",
		[]string{"domain", "source_file", "target_device"},
		nil)
	libvirtDomainBlockPhysicalSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_block_stats", "physicalsize"),
		"Physical size in bytes of the container of the backing image.",
		[]string{"domain", "source_file", "target_device"},
		nil)

	libvirtDomainInterfaceRxBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_interface_stats", "receive_bytes_total"),
		"Number of bytes received on a network interface, in bytes.",
		[]string{"domain", "source_bridge", "target_device", "virtualportinterfaceid"},
		nil)
	libvirtDomainInterfaceRxPacketsDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_interface_stats", "receive_packets_total"),
		"Number of packets received on a network interface.",
		[]string{"domain", "source_bridge", "target_device", "virtualportinterfaceid"},
		nil)
	libvirtDomainInterfaceRxErrsDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_interface_stats", "receive_errors_total"),
		"Number of packet receive errors on a network interface.",
		[]string{"domain", "source_bridge", "target_device", "virtualportinterfaceid"},
		nil)
	libvirtDomainInterfaceRxDropDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_interface_stats", "receive_drops_total"),
		"Number of packet receive drops on a network interface.",
		[]string{"domain", "source_bridge", "target_device", "virtualportinterfaceid"},
		nil)
	libvirtDomainInterfaceTxBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_interface_stats", "transmit_bytes_total"),
		"Number of bytes transmitted on a network interface, in bytes.",
		[]string{"domain", "source_bridge", "target_device", "virtualportinterfaceid"},
		nil)
	libvirtDomainInterfaceTxPacketsDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_interface_stats", "transmit_packets_total"),
		"Number of packets transmitted on a network interface.",
		[]string{"domain", "source_bridge", "target_device", "virtualportinterfaceid"},
		nil)
	libvirtDomainInterfaceTxErrsDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_interface_stats", "transmit_errors_total"),
		"Number of packet transmit errors on a network interface.",
		[]string{"domain", "source_bridge", "target_device", "virtualportinterfaceid"},
		nil)
	libvirtDomainInterfaceTxDropDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_interface_stats", "transmit_drops_total"),
		"Number of packet transmit drops on a network interface.",
		[]string{"domain", "source_bridge", "target_device", "virtualportinterfaceid"},
		nil)

	libvirtDomainMemoryStatMajorfaultDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_memory_stats", "major_fault"),
		"Page faults occur when a process makes a valid access to virtual memory that is not available. "+
			"When servicing the page fault, if disk IO is required, it is considered a major fault.",
		[]string{"domain"},
		nil)
	libvirtDomainMemoryStatMinorFaultDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_memory_stats", "minor_fault"),
		"Page faults occur when a process makes a valid access to virtual memory that is not available. "+
			"When servicing the page not fault, if disk IO is required, it is considered a minor fault.",
		[]string{"domain"},
		nil)
	libvirtDomainMemoryStatUnusedDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_memory_stats", "unused"),
		"The amount of memory left completely unused by the system. Memory that is available but used for "+
			"reclaimable caches should NOT be reported as free. This value is expressed in kB.",
		[]string{"domain"},
		nil)
	libvirtDomainMemoryStatAvailableDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_memory_stats", "available"),
		"The total amount of usable memory as seen by the domain. This value may be less than the amount of "+
			"memory assigned to the domain if a balloon driver is in use or if the guest OS does not initialize all "+
			"assigned pages. This value is expressed in kB.",
		[]string{"domain"},
		nil)
	libvirtDomainMemoryStatActualBaloonDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_memory_stats", "actual_balloon"),
		"Current balloon value (in KB).",
		[]string{"domain"},
		nil)
	libvirtDomainMemoryStatRssDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_memory_stats", "rss"),
		"Resident Set Size of the process running the domain. This value is in kB",
		[]string{"domain"},
		nil)
	libvirtDomainMemoryStatUsableDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_memory_stats", "usable"),
		"How much the balloon can be inflated without pushing the guest system to swap, corresponds "+
			"to 'Available' in /proc/meminfo",
		[]string{"domain"},
		nil)
	libvirtDomainMemoryStatDiskCachesDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_memory_stats", "disk_cache"),
		"The amount of memory, that can be quickly reclaimed without additional I/O (in kB)."+
			"Typically these pages are used for caching files from disk.",
		[]string{"domain"},
		nil)
	libvirtDomainMemoryStatUsedPercentDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_memory_stats", "used_percent"),
		"The amount of memory in percent, that used by domain.",
		[]string{"domain"},
		nil)

	libvirtDomainInfoCPUStealTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName("libvirt", "domain_info", "cpu_steal_time_total"),
		"Amount of CPU time stolen from the domain, in ns, that is, 1/1,000,000,000 of a second, or 10−9 seconds.",
		[]string{"domain", "cpu"},
		nil)
)

type Domain struct {
	Devices Devices `xml:"devices"`
}

type Devices struct {
	Disks      []Disk      `xml:"disk"`
	Interfaces []Interface `xml:"interface"`
}

type Disk struct {
	Device   string     `xml:"device,attr"`
	Source   DiskSource `xml:"source"`
	Target   DiskTarget `xml:"target"`
	DiskType string     `xml:"type,attr"`
}

type DiskSource struct {
	File string `xml:"file,attr"`
	Name string `xml:"name,attr"`
}

type DiskTarget struct {
	Device string `xml:"dev,attr"`
}

type Interface struct {
	Source      InterfaceSource      `xml:"source"`
	Target      InterfaceTarget      `xml:"target"`
	Virtualport InterfaceVirtualPort `xml:"virtualport"`
}

type InterfaceVirtualPort struct {
	Parameters InterfaceVirtualPortParam `xml:"parameters"`
}
type InterfaceVirtualPortParam struct {
	InterfaceId string `xml:"interfaceid,attr"`
}

type InterfaceSource struct {
	Bridge string `xml:"bridge,attr"`
}

type InterfaceTarget struct {
	Device string `xml:"dev,attr"`
}

type VirDomainMemoryStats struct {
	Major_fault    uint64
	Minor_fault    uint64
	Unused         uint64
	Available      uint64
	Actual_balloon uint64
	Rss            uint64
	Usable         uint64
	Disk_caches    uint64
}

// QueryCPUsResult holds the structured representative of QMP's "query-cpus" output
type QueryCPUsResult struct {
	Return []QemuThread `json:"return"`
}

// QemuThread holds qemu thread info: which virtual cpu is it, what the thread PID is
type QemuThread struct {
	CPU      int
	ThreadID int `json:"thread_id"`
}

// ReadStealTime reads the file /proc/<thread_id>/schedstat and returns
// the second field as a float64 value
func (e *LibvirtExporter) readStealTime(pid int) (float64, error) {
	var retval float64
	path := fmt.Sprintf("%s/%d/schedstat", e.hostProcfs, pid)
	result, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	values := strings.Split(string(result), " ")
	// We expect exactly 3 fields in the output, otherwise we return error
	if len(values) != 3 {
		return 0, fmt.Errorf("%s: not exactly 3 fields: %s", path, result)
	}

	retval, err = strconv.ParseFloat(values[1], 64)
	if err != nil {
		return 0, err
	}

	return retval, nil
}

// collectDomainStealTime contacts the running QEMU instance via QemuMonitorCommand API call,
// gets the PIDs of the running CPU threads.
// It then calls ReadStealTime for every thread to obtain its steal times
func (e *LibvirtExporter) collectDomainStealTime(ch chan<- prometheus.Metric, domain libvirt.Domain) error {
	var totalStealTime float64

	// query QEMU directly to ask PID numbers of its CPU threads
	resultJSON, err := e.conn.QEMUDomainMonitorCommand(domain, "{\"execute\": \"query-cpus\"}", 0)
	if err != nil {
		return err
	}
	// Allocate a map for the json parser results
	qemuThreadsResult := QueryCPUsResult{Return: make([]QemuThread, 0, 8)}

	// Parse the result into the map
	err = json.Unmarshal([]byte(resultJSON), &qemuThreadsResult)
	if err != nil {
		return err
	}

	// Now iterate over qemuThreadsResult to get the list of QemuThread
	for _, thread := range qemuThreadsResult.Return {
		stealTime, err := e.readStealTime(thread.ThreadID)
		if err != nil {
			slog.Warn("Fetching steal time failed. Skipping", "thread_id", thread.ThreadID, "err", err)
			continue
		}
		// Increment the total steal time
		totalStealTime += stealTime

		// Send the metric for this CPU
		ch <- prometheus.MustNewConstMetric(libvirtDomainInfoCPUStealTimeDesc, prometheus.CounterValue, stealTime, domain.Name, fmt.Sprintf("%d", thread.CPU))
	}
	ch <- prometheus.MustNewConstMetric(libvirtDomainInfoCPUStealTimeDesc, prometheus.CounterValue, totalStealTime, domain.Name, "total")
	return nil
}

// collectDomain extracts Prometheus metrics from a libvirt domain.
func (e *LibvirtExporter) collectDomain(ch chan<- prometheus.Metric, stat libvirt.DomainStatsRecord) error {
	dom := stat.Dom
	domainName := dom.Name
	// Decode XML description of domain to get block device names, etc.
	xmlDesc, err := e.conn.DomainGetXMLDesc(dom, 0)
	if err != nil {
		return err
	}

	var desc Domain
	err = xml.Unmarshal([]byte(xmlDesc), &desc)
	if err != nil {
		return err
	}

	// Report domain info.
	rState, rMaxMem, rMemory, rNrVirtCPU, rCPUTime, err := e.conn.DomainGetInfo(dom)
	if err != nil {
		return err
	}
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainInfoMaxMemDesc,
		prometheus.GaugeValue,
		float64(rMaxMem)*1024,
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainInfoMemoryUsageDesc,
		prometheus.GaugeValue,
		float64(rMemory)*1024,
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainInfoNrVirtCpuDesc,
		prometheus.GaugeValue,
		float64(rNrVirtCPU),
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainInfoCpuTimeDesc,
		prometheus.CounterValue,
		float64(rCPUTime)/1e9,
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainInfoVirDomainState,
		prometheus.CounterValue,
		float64(rState),
		domainName)

	params := map[string]any{}
	for _, p := range stat.Params {
		params[p.Field] = p.Value.I
	}

	// Report block device statistics.
	for i := range params["block.count"].(uint32) {
		var diskSource string
		diskName := params[fmt.Sprintf("block.%d.name", i)].(string)
		if diskName == "hdc" {
			continue
		}
		diskPath := params[fmt.Sprintf("block.%d.path", i)].(string)
		/*  "block.<num>.path" - string describing the source of block device <num>,
		    if it is a file or block device (omitted for network
		    sources and drives with no media inserted). For network device (i.e. rbd) take from xml. */
		for _, dev := range desc.Devices.Disks {
			if dev.Target.Device == diskName {
				if diskPath != "" {
					diskSource = diskPath
				} else {
					diskSource = dev.Source.Name
				}
				break
			}
		}

		// https://libvirt.org/html/libvirt-libvirt-domain.html#virConnectGetAllDomainStats
		diskRdBytes, ok := params[fmt.Sprintf("block.%d.rd.bytes", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockRdBytesDesc,
				prometheus.CounterValue,
				float64(diskRdBytes),
				domainName,
				diskSource,
				diskName)
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockRdTotalTimesDesc,
				prometheus.CounterValue,
				float64(diskRdBytes)/1e9,
				domainName,
				diskSource,
				diskName)
		}
		diskRdReqs, ok := params[fmt.Sprintf("block.%d.rd.reqs", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockRdReqDesc,
				prometheus.CounterValue,
				float64(diskRdReqs),
				domainName,
				diskSource,
				diskName)
		}
		diskWrBytes, ok := params[fmt.Sprintf("block.%d.wr.bytes", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockWrBytesDesc,
				prometheus.CounterValue,
				float64(diskWrBytes),
				domainName,
				diskSource,
				diskName)
		}
		diskWrReqs, ok := params[fmt.Sprintf("block.%d.wr.reqs", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockWrReqDesc,
				prometheus.CounterValue,
				float64(diskWrReqs),
				domainName,
				diskSource,
				diskName)
		}
		diskWrTimes, ok := params[fmt.Sprintf("block.%d.wr.times", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockWrTotalTimesDesc,
				prometheus.CounterValue,
				float64(diskWrTimes)/1e9,
				domainName,
				diskSource,
				diskName)
		}
		diskFlReqs, ok := params[fmt.Sprintf("block.%d.fl.reqs", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockFlushReqDesc,
				prometheus.CounterValue,
				float64(diskFlReqs),
				domainName,
				diskSource,
				diskName)
		}
		diskFlTimes, ok := params[fmt.Sprintf("block.%d.fl.times", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockFlushTotalTimesDesc,
				prometheus.CounterValue,
				float64(diskFlTimes),
				domainName,
				diskSource,
				diskName)
		}
		diskAllocation, ok := params[fmt.Sprintf("block.%d.allocation", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockAllocationDesc,
				prometheus.CounterValue,
				float64(diskAllocation),
				domainName,
				diskSource,
				diskName)
		}
		diskCapacity, ok := params[fmt.Sprintf("block.%d.capacity", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockCapacityDesc,
				prometheus.CounterValue,
				float64(diskCapacity),
				domainName,
				diskSource,
				diskName)
		}
		diskPhysical, ok := params[fmt.Sprintf("block.%d.physical", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainBlockPhysicalSizeDesc,
				prometheus.CounterValue,
				float64(diskPhysical),
				domainName,
				diskSource,
				diskName)
		}
	}

	// Report network interface statistics.
	for i := range params["net.count"].(uint32) {
		var sourceBridge string
		var virtualPortInterfaceID string
		ifaceName, ok := params[fmt.Sprintf("net.%d.name", i)].(string)
		if !ok {
			continue
		}
		// Additional info for ovs network
		for _, net := range desc.Devices.Interfaces {
			if net.Target.Device == ifaceName {
				sourceBridge = net.Source.Bridge
				virtualPortInterfaceID = net.Virtualport.Parameters.InterfaceId
				break
			}
		}
		ifaceRxBytes, ok := params[fmt.Sprintf("net.%d.rx.bytes", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainInterfaceRxBytesDesc,
				prometheus.CounterValue,
				float64(ifaceRxBytes),
				domainName,
				sourceBridge,
				ifaceName,
				virtualPortInterfaceID)
		}
		ifaceRxPkts, ok := params[fmt.Sprintf("net.%d.rx.pkts", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainInterfaceRxPacketsDesc,
				prometheus.CounterValue,
				float64(ifaceRxPkts),
				domainName,
				sourceBridge,
				ifaceName,
				virtualPortInterfaceID)
		}
		ifaceRxErrs, ok := params[fmt.Sprintf("net.%d.rx.errs", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainInterfaceRxErrsDesc,
				prometheus.CounterValue,
				float64(ifaceRxErrs),
				domainName,
				sourceBridge,
				ifaceName,
				virtualPortInterfaceID)
		}
		ifaceRxDrop, ok := params[fmt.Sprintf("net.%d.rx.drop", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainInterfaceRxDropDesc,
				prometheus.CounterValue,
				float64(ifaceRxDrop),
				domainName,
				sourceBridge,
				ifaceName,
				virtualPortInterfaceID)
		}
		ifaceTxBytes, ok := params[fmt.Sprintf("net.%d.tx.bytes", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainInterfaceTxBytesDesc,
				prometheus.CounterValue,
				float64(ifaceTxBytes),
				domainName,
				sourceBridge,
				ifaceName,
				virtualPortInterfaceID)
		}
		ifaceTxPkts, ok := params[fmt.Sprintf("net.%d.tx.pkts", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainInterfaceTxPacketsDesc,
				prometheus.CounterValue,
				float64(ifaceTxPkts),
				domainName,
				sourceBridge,
				ifaceName,
				virtualPortInterfaceID)
		}
		ifaceTxErrs, ok := params[fmt.Sprintf("net.%d.tx.errs", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainInterfaceTxErrsDesc,
				prometheus.CounterValue,
				float64(ifaceTxErrs),
				domainName,
				sourceBridge,
				ifaceName,
				virtualPortInterfaceID)
		}
		ifaceTxDrop, ok := params[fmt.Sprintf("net.%d.tx.drop", i)].(uint64)
		if ok {
			ch <- prometheus.MustNewConstMetric(
				libvirtDomainInterfaceTxDropDesc,
				prometheus.CounterValue,
				float64(ifaceTxDrop),
				domainName,
				sourceBridge,
				ifaceName,
				virtualPortInterfaceID)
		}
	}

	// Collect Memory Stats
	memorystat, err := e.conn.DomainMemoryStats(dom, 11, 0)
	var MemoryStats VirDomainMemoryStats
	var used_percent float64
	if err == nil {
		MemoryStats = MemoryStatCollect(&memorystat)
		if MemoryStats.Usable != 0 && MemoryStats.Available != 0 {
			used_percent = (float64(MemoryStats.Available) - float64(MemoryStats.Usable)) / (float64(MemoryStats.Available) / float64(100))
		}

	}
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainMemoryStatMajorfaultDesc,
		prometheus.CounterValue,
		float64(MemoryStats.Major_fault),
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainMemoryStatMinorFaultDesc,
		prometheus.CounterValue,
		float64(MemoryStats.Minor_fault),
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainMemoryStatUnusedDesc,
		prometheus.CounterValue,
		float64(MemoryStats.Unused),
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainMemoryStatAvailableDesc,
		prometheus.CounterValue,
		float64(MemoryStats.Available),
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainMemoryStatActualBaloonDesc,
		prometheus.CounterValue,
		float64(MemoryStats.Actual_balloon),
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainMemoryStatRssDesc,
		prometheus.CounterValue,
		float64(MemoryStats.Rss),
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainMemoryStatUsableDesc,
		prometheus.CounterValue,
		float64(MemoryStats.Usable),
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainMemoryStatDiskCachesDesc,
		prometheus.CounterValue,
		float64(MemoryStats.Disk_caches),
		domainName)
	ch <- prometheus.MustNewConstMetric(
		libvirtDomainMemoryStatUsedPercentDesc,
		prometheus.CounterValue,
		float64(used_percent),
		domainName)

	return nil
}

func MemoryStatCollect(memorystat *[]libvirt.DomainMemoryStat) VirDomainMemoryStats {
	var MemoryStats VirDomainMemoryStats
	for _, domainmemorystat := range *memorystat {
		switch tag := domainmemorystat.Tag; tag {
		case 2:
			MemoryStats.Major_fault = domainmemorystat.Val
		case 3:
			MemoryStats.Minor_fault = domainmemorystat.Val
		case 4:
			MemoryStats.Unused = domainmemorystat.Val
		case 5:
			MemoryStats.Available = domainmemorystat.Val
		case 6:
			MemoryStats.Actual_balloon = domainmemorystat.Val
		case 7:
			MemoryStats.Rss = domainmemorystat.Val
		case 8:
			MemoryStats.Usable = domainmemorystat.Val
		case 10:
			MemoryStats.Disk_caches = domainmemorystat.Val
		}
	}
	return MemoryStats
}

// LibvirtExporter implements a Prometheus exporter for libvirt state.
type LibvirtExporter struct {
	uri        string
	hostProcfs string
	conn       *libvirt.Libvirt
}

// NewLibvirtExporter creates a new Prometheus exporter for libvirt.
func NewLibvirtExporter(uri string, procfs string) (*LibvirtExporter, error) {
	if uri == "" {
		uri = "qemu:///system"
	}
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	lv, err := libvirt.ConnectToURI(u)
	if err != nil {
		return nil, err
	}
	return &LibvirtExporter{
		uri:        uri,
		conn:       lv,
		hostProcfs: procfs,
	}, nil
}

// Describe returns metadata for all Prometheus metrics that may be exported.
func (e *LibvirtExporter) Describe(ch chan<- *prometheus.Desc) {
	// Status
	ch <- libvirtUpDesc

	// Domain info
	ch <- libvirtDomainInfoMaxMemDesc
	ch <- libvirtDomainInfoMemoryUsageDesc
	ch <- libvirtDomainInfoNrVirtCpuDesc
	ch <- libvirtDomainInfoCpuTimeDesc
	ch <- libvirtDomainInfoCPUStealTimeDesc
	ch <- libvirtDomainInfoVirDomainState

	// Domain block stats
	ch <- libvirtDomainBlockRdBytesDesc
	ch <- libvirtDomainBlockRdReqDesc
	ch <- libvirtDomainBlockRdTotalTimesDesc
	ch <- libvirtDomainBlockWrBytesDesc
	ch <- libvirtDomainBlockWrReqDesc
	ch <- libvirtDomainBlockWrTotalTimesDesc
	ch <- libvirtDomainBlockFlushReqDesc
	ch <- libvirtDomainBlockFlushTotalTimesDesc
	ch <- libvirtDomainBlockAllocationDesc
	ch <- libvirtDomainBlockCapacityDesc
	ch <- libvirtDomainBlockPhysicalSizeDesc

	// Domain net interfaces stats
	ch <- libvirtDomainInterfaceRxBytesDesc
	ch <- libvirtDomainInterfaceRxPacketsDesc
	ch <- libvirtDomainInterfaceRxErrsDesc
	ch <- libvirtDomainInterfaceRxDropDesc
	ch <- libvirtDomainInterfaceTxBytesDesc
	ch <- libvirtDomainInterfaceTxPacketsDesc
	ch <- libvirtDomainInterfaceTxErrsDesc
	ch <- libvirtDomainInterfaceTxDropDesc

	// Domain memory stats
	ch <- libvirtDomainMemoryStatMajorfaultDesc
	ch <- libvirtDomainMemoryStatMinorFaultDesc
	ch <- libvirtDomainMemoryStatUnusedDesc
	ch <- libvirtDomainMemoryStatAvailableDesc
	ch <- libvirtDomainMemoryStatActualBaloonDesc
	ch <- libvirtDomainMemoryStatRssDesc
	ch <- libvirtDomainMemoryStatUsableDesc
	ch <- libvirtDomainMemoryStatDiskCachesDesc
}

// Collect scrapes Prometheus metrics from libvirt.
func (e *LibvirtExporter) Collect(ch chan<- prometheus.Metric) {
	err := e.collectFromLibvirt(ch)
	if err == nil {
		ch <- prometheus.MustNewConstMetric(
			libvirtUpDesc,
			prometheus.GaugeValue,
			1.0)
	} else {
		slog.Warn("Failed to scrape metrics", "err", err)
		ch <- prometheus.MustNewConstMetric(
			libvirtUpDesc,
			prometheus.GaugeValue,
			0.0)
	}
}

func (e *LibvirtExporter) Close() {
	e.conn.ConnectClose()
}

// collectFromLibvirt obtains Prometheus metrics from all domains in a libvirt setup.
func (e *LibvirtExporter) collectFromLibvirt(ch chan<- prometheus.Metric) error {
	stats, err := e.conn.ConnectGetAllDomainStats([]libvirt.Domain{}, uint32(libvirt.DomainStatsState|
		libvirt.DomainStatsCPUTotal|
		libvirt.DomainStatsInterface|
		libvirt.DomainStatsBalloon|
		libvirt.DomainStatsBlock|
		libvirt.DomainStatsPerf|
		libvirt.DomainStatsVCPU), 0)
	if err != nil {
		return err
	}
	for _, stat := range stats {
		err = e.collectDomain(ch, stat)
		if err != nil {
			slog.Warn("Failed to collect domain", "domain", stat.Dom.Name, "err", err)
			continue
		}
		if e.hostProcfs != "" {
			err = e.collectDomainStealTime(ch, stat.Dom)
			if err != nil {
				slog.Warn("Failed to collect steal time", "domain", stat.Dom.Name, "err", err)
				continue
			}
		}
	}
	return nil
}

func main() {
	defaultAddr := os.Getenv("LIBVIRT_EXPORTER_LISTEN")
	if defaultAddr == "" {
		defaultAddr = ":9177"
	}
	listenAddress := flag.String("listen", defaultAddr, "Address to listen on for web interface and telemetry.")
	metricsPath := flag.String("path", "/metrics", "Path under which to expose metrics.")
	libvirtURI := flag.String("uri", os.Getenv("LIBVIRT_EXPORTER_URI"), "Libvirt URI from which to extract metrics.")
	procfs := flag.String("procfs", os.Getenv("LIBVIRT_EXPORTER_PROCFS"), "Path where `/proc` from host is mounted. Usually `/host/proc` in containerized environments.")
	flag.Parse()

	exporter, err := NewLibvirtExporter(*libvirtURI, *procfs)
	if err != nil {
		panic(err)
	}
	prometheus.MustRegister(exporter)
	slog.Info("Initialized libvirt-exporter", "listenAddress", *listenAddress)

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`
			<html>
			<head><title>Libvirt Exporter</title></head>
			<body>
			<h1>Libvirt Exporter</h1>
			<p><a href='` + *metricsPath + `'>Metrics</a></p>
			</body>
			</html>`))
	})
	err = http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		slog.Error("listen", "err", err)
	}
}
