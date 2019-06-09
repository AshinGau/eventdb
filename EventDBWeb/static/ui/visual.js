let globalDB;
let stackChart = echarts.init(document.getElementById('stackArea'), 'wonderland');
let pieChart = echarts.init(document.getElementById('pieArea'), 'wonderland');
let groupChart = echarts.init(document.getElementById('groupArea'), 'wonderland');
let barChart = echarts.init(document.getElementById('barArea'), 'wonderland');
let rankChart = echarts.init(document.getElementById('rankArea'), 'wonderland');
let stackOption = {
    tooltip : {
        trigger: 'axis',
        axisPointer: {
            type: 'cross',
            label: {
                backgroundColor: '#6a7985'
            }
        }
    },
    legend: {
        align: 'left',
        orient: 'vertical',
        right: 'left',
        top: '30%',
        tooltip: {
            show: true
        },
        formatter: name => echarts.format.truncateText(name, 80, '14px Microsoft Yahei', '…')
    },
    grid: {
        top: 20,
        left: 0,
        right: '130',
        bottom: '3%',
        containLabel: true
    },
    xAxis : [
        {
            type : 'category',
            boundaryGap : false
        }
    ],
    yAxis : [
        {
            type : 'value'
        }
    ]
};
let pieOption = {
    tooltip : {
        trigger: 'item'
    },
    series : [
        {
            name: '机时分布',
            type: 'pie',
            radius : '60%',
            center: ['40%', '46%'],
            itemStyle: {
                emphasis: {
                    shadowBlur: 10,
                    shadowOffsetX: 0,
                    shadowColor: 'rgba(0, 0, 0, 0.5)'
                }
            }
        }
    ]
};
let barOption = {
    tooltip : {
        trigger: 'axis',
        axisPointer : {
            type : 'shadow'
        }
    },
    legend: {
        data: ['机时', '人次']
    },
    grid: {
        top: 30,
        left: 0,
        right: '4%',
        bottom: '5%',
        containLabel: true
    },
    xAxis : [
        {
            type : 'category',
            axisTick: {
                alignWithLabel: true
            }
        }
    ],
    yAxis : [
        {
            type : 'value'
        }
    ],
    series : [
        {
            name:'机时',
            type:'bar',
            barWidth: '50%'
        },
        {
            name:'人次',
            type:'line',
        }
    ]
};
let groupOption = {
    tooltip : {
        trigger: 'axis',
        axisPointer : {
            type : 'shadow'
        }
    },
    grid: {
        top: 20,
        left: 0,
        right: '4%',
        bottom: 50,
        containLabel: true
    },
    xAxis : [
        {
            type : 'category',
            axisTick: {
                alignWithLabel: true
            }
        }
    ],
    yAxis : [
        {
            type : 'value'
        }
    ],
    series : [
        {
            name:'机时',
            type:'bar',
            barWidth: '30%'
        }
    ]
};
let rankOption = {
    tooltip: {
        trigger: 'axis',
        axisPointer: {
            type: 'shadow'
        }
    },
    grid: {
        top: 20,
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
    },
    xAxis: {
        type: 'value',
        boundaryGap: [0, 0.01],
        position: 'top'
    },
    yAxis: {
        type: 'category'
    },
    series: [
        {
            type: 'bar',
            barWidth: '50%'
        }
    ]
};

function getInstruInfo(start, end, equips, num){
	stackChart.showLoading();
	pieChart.showLoading();
	groupChart.showLoading();
	barChart.showLoading();
	rankChart.showLoading();
    let $def = $.Deferred();
    $.ajax({
   		type:'get',
   		url:'retriveEChartData.action',
   		data:{
   			startDateTime: start,
   			endDateTime: end,
   			instrumentsId: equips
   		},
   		dataType:'json',
   		success: function(data){
   			let group = JSON.parse(data.jsonResult.Group),
   				rank = JSON.parse(data.jsonResult.Rank),
   				equipments = JSON.parse(data.jsonResult.equipments);
   			let i = 0, db = [];
   			while(i < equipments.length){
   				let tmp = {};
   				tmp.date = equipments[i].months;
   				for(let j = 0; j < num; j++, i++){
   					let obj = {};
   					obj['hours'] = equipments[i].hours;
   					obj['cnts'] = equipments[i].count;
   					tmp[equipments[i].name] = obj;
   				}
   				db.push(tmp);
   			}
   			//console.log(erNames);
   			//console.log(db);
   			$def.resolve(erNames, db, rank, group);
   		},
   		error: function(err){
   			$def.reject(err);
   		}
   	});
    return $def.promise();
}


Date.prototype.format = function(fmt) {
    var o = {
        "M+" : this.getMonth()+1,                 //月份
        "d+" : this.getDate(),                    //日
        "h+" : this.getHours(),                   //小时
        "m+" : this.getMinutes(),                 //分
        "s+" : this.getSeconds(),                 //秒
        "q+" : Math.floor((this.getMonth()+3)/3), //季度
        "S"  : this.getMilliseconds()             //毫秒
    };
    if(/(y+)/.test(fmt)) {
        fmt=fmt.replace(RegExp.$1, (this.getFullYear()+"").substr(4 - RegExp.$1.length));
    }
    for(var k in o) {
        if(new RegExp("("+ k +")").test(fmt)){
            fmt = fmt.replace(RegExp.$1, (RegExp.$1.length==1) ? (o[k]) : (("00"+ o[k]).substr((""+ o[k]).length)));
        }
    }
    return fmt;
}

let $sdInput = $('#report-start-date'),
    $edInput = $('#report-end-date'),
    $dPicker = $('#ui-datepicker-div'),
    $dPrev = $dPicker.find('.ui-datepicker-prev'),
    $dNext = $dPicker.find('.ui-datepicker-next'),
    $dPickerDate = $dPicker.find('.ui-datepicker-title'),
    dPickerState = false;
    dNow = new Date(),
    dNowPrev6 = new Date();

dNowPrev6.setMonth(dNowPrev6.getMonth() - 6);
dNowPrev6.setDate(1);
$sdInput.val(dNowPrev6.format('yyyy-MM'));
$edInput.val(dNow.format('yyyy-MM'));

function dInputFocus(){
    let $this = $(this),
        left = $this.offset().left,
        top = $this.offset().top + 27;
    $dPickerDate.text($this.val());
    $dPicker.css({
        top: top,
        left: left
    }).show();
    dPickerState = true;
    $dPicker.__target = $this;
}

$dPrev.click(function(){
    $input = $dPicker.__target;
    dd = new Date($input.val());
    dd.setMonth(dd.getMonth() - 1);
    $input.val(dd.format('yyyy-MM'));
    $dPickerDate.text(dd.format('yyyy-MM'));
});

$dNext.click(function(){
    $input = $dPicker.__target;
    dd = new Date($input.val());
    dd.setMonth(dd.getMonth() + 1);
    $input.val(dd.format('yyyy-MM'));
    $dPickerDate.text(dd.format('yyyy-MM'));
});

$(window).click(function(e){
    if(dPickerState){
        let $target = $(e.target);
        if( !($target.closest($dPicker).length > 0 || $target.is($dPicker.__target))){
            $dPicker.hide();
            $dPicker.__target = null;
        }
    }
});

function monthPick($input){
    $input.focus(dInputFocus);
}
monthPick($sdInput);
monthPick($edInput);

let $renderDetailBox = $('#renderDetailBox'),
	$renderDetailTitle = $('#renderDetailTitle');
function renderDetail(){
	$renderDetailBox.empty();
	for(let i = 0; i < erNames.length; i++){
		$renderDetailBox.append($('<li>').append($('<a>').text(erNames[i])));
	}
	$renderDetailBox.find('li').first().addClass('hover');
	$renderDetailTitle.text(erNames[0] + '使用详情');
	
	let $li = $renderDetailBox.find('li');
	$li.click(function(){
		$li.removeClass('hover');
		let $self = $(this);
		$self.addClass('hover');
		renderBarView(globalDB, $self.text());
		$renderDetailTitle.text($self.text() + '使用详情');
	})
}

function renderBarView(data, field){
	//柱状图
	barChart.showLoading();
    barOption.series[0].data = data.map(d => d[field].hours);
    barOption.series[1].data = data.map(d => d[field].cnts);
    barOption.xAxis[0].data = data.map(d => d.date);
    barChart.hideLoading();
    barChart.setOption(barOption);
}

function renderStackView(data, title, stack){
	//堆积图
	let stackName = 'stack';
	stack.showLoading();
    stackOption.xAxis[0].data = data.map(d => d.date);
    let legend = title;
    stackOption.legend.data = legend.map(d => {
        return {
            name: d,
            icon: 'rect'
        }
    });
    let series = legend.map(lg => {
        let tmp = {
            name: lg,
            type:'line',
            stack: stackName,
            areaStyle: {normal: {}},
            //data:[],
            label: {
                emphasis: {
                    show: true,
                    position: 'top',
                    textStyle:{
                        fontSize: 15
                    }
                }
            }
        };
        tmp.data = data.map(rd => rd[lg].hours);
        return tmp;
    });
    stackOption.series = series;
    stack.hideLoading();
    stack.setOption(stackOption);
}

function renderView(title, data, rank, group){
	let $txt = $('#show-detail-text'),
		$showAll = $('#show-all'),
		allHours = 0,
		allHoursArr = [],
		allCnts = 0,
		legend = title; 
	for(let val of data){
		let sum = 0;
		for(let key of erNames){
			allHours += val[key].hours;
			allCnts += val[key].cnts;
			sum += val[key].hours;
		}
		let obj = {};
		obj.date = val['date'];
		obj['总计'] = {'hours': sum, 'cnts': 0};
		allHoursArr.push(obj);
	}
	$txt.text(`累积在线预约设备${erNames.length}台，累积预约机时${allHours}小时，预约${allCnts}人次。`);
	$showAll.change(function(){
		if($(this).attr('checked')){
			stackChart.dispose();
			stackChart = echarts.init(document.getElementById('stackArea'), 'wonderland');
			renderStackView(data, title, stackChart);
		}
		else{
			stackChart.dispose();
			stackChart = echarts.init(document.getElementById('stackArea'), 'wonderland');
			renderStackView(allHoursArr, ['总计'], stackChart);
		}
	});
	
	
	globalDB = data;
	renderDetail();
	//堆积图
    renderStackView(data, title, stackChart);

    //饼图
    pieOption.tooltip.formatter = `${data[0].date} - ${data[data.length - 1].date}<br/>{b} : {c}小时 ({d}%)`;
    let pieData = [];
    for(let key of legend){
        let pieObj = {};
        pieObj.name = key;
        pieObj.value = 0;
        for(let val of data)
            pieObj.value += val[key].hours;
        pieData.push(pieObj);
    }
    pieOption.series[0].data = pieData;
    pieChart.hideLoading();
    pieChart.setOption(pieOption);

    //柱状图
    renderBarView(data, erNames[0]);
    
    //group
    groupOption.series[0].data = group.map(d => d.hours);
    groupOption.xAxis[0].data = group.map(d => d.name);
    groupChart.hideLoading();
    groupChart.setOption(groupOption);
    
    //rank
    rank = rank.sort((a, b) => {
        return a.hours - b.hours;
     });
     rankOption.series[0].data = rank.map(d => d.hours);
     rankOption.yAxis.data = rank.map(d => d.name);
     rankChart.hideLoading();
     rankChart.setOption(rankOption);
}

//init
let ddNow = new Date();
ddNow.setMonth(ddNow.getMonth() + 1);
getInstruInfo(dNowPrev6.format('yyyy-MM-dd'), ddNow.format('yyyy-MM-dd'), equipmentRequest.join(), equipmentRequest.length).done(renderView);

$('#searchBtn').click(function(){
	let dstart = new Date($('#report-start-date').val()),
		dend = new Date($('#report-end-date').val()),
		$getesb = $('.equipmentSelectCheckbox');
	if((dstart - dend) > 0){
		alert('日期选择错误');
		return;
	}
	
	dend.setMonth(dend.getMonth() + 1);
	equipmentRequest = [];
	erNames = [];
	erLength = 0;
	$getesb.each(function(){
		let $self = $(this);
		if($self.attr('checked')){
			equipmentRequest.push($self.val());
			erNames.push($self.data('name'));
		}
	});
	erLength = erNames.length;
	if(erLength < 1){
		alert('请至少选择一个设备');
		return;
	}
	stackChart.dispose();
	stackChart = echarts.init(document.getElementById('stackArea'), 'wonderland');
	getInstruInfo(dstart.format('yyyy-MM-dd'), dend.format('yyyy-MM-dd'), equipmentRequest.join(), equipmentRequest.length).done(renderView);
})