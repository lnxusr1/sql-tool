import { api_url } from '/settings.js';
var isMouseDown = false;
var activeArea = "editor";
var tabIncrement = 1;
var escapeKey = false;
var auto_timer = null;

function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
    .replace(/[xy]/g, function (c) {
        const r = Math.random() * 16 | 0, 
            v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

function addSidebarIcon(obj, iconClass, weight) {
    if (weight === undefined) { weight = "fas"; }

    $(obj).each(function (i, o) {
        $(o).children('button').children('i').eq(1).removeClass('fas');
        $(o).children('button').children('i').eq(1).removeClass('far');
        $(o).children('button').children('i').eq(1).addClass(weight);
        $(o).children('button').children('i').eq(1).addClass('fa-fw');
        $(o).children('button').children('i').eq(1).addClass(iconClass);
    });
}

function fixIconClasses() {
    addSidebarIcon($('sidebar > div > div > ul > li'), 'fa-server');
    addSidebarIcon($('sidebar > div > div > ul > li > ul > li'), 'fa-database');
    addSidebarIcon($('sidebar > div > div > ul > li > ul > li > ul > li'), 'fa-folder');

    $('sidebar > div > div > ul > li > ul > li > ul').children('li').each(function (i, o) { 
        if (i % 2 == 0) { addSidebarIcon($(o).children('ul').children('li'), 'fa-file-lines'); }
        if (i % 2 == 1) { addSidebarIcon($(o).children('ul').children('li'), 'fa-user'); }
    });

    addSidebarIcon($('sidebar > div > div > ul > li > ul > li > ul > li > ul > li > ul > li'), 'fa-folder');


    $('sidebar > div > div > ul > li > ul > li > ul > li > ul > li > ul').children('li').each(function (i, o) { 
        if (i % 2 == 0) { addSidebarIcon($(o).children('ul').children('li'), 'fa-table'); }
        if (i % 2 == 1) { addSidebarIcon($(o).children('ul').children('li'), 'fa-layer-group'); }
    });

    addSidebarIcon($('sidebar > div > div > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li'), 'fa-folder', 'far');

    $('sidebar > div > div > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul').children('li').each(function (i, o) { 
        if (i % 3 == 0) { addSidebarIcon($(o).children('ul').children('li'), 'fa-columns'); }
        if (i % 3 != 0) { addSidebarIcon($(o).children('ul').children('li'), 'fa-file-lines', 'far'); }
    });

    addSidebarIcon($('sidebar > div > div > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li'), 'fa-folder', 'far');

    $('sidebar > div > div > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul').children('li').each(function (i, o) { 
        if (i % 3 == 0) { addSidebarIcon($(o).children('ul').children('li'), 'fa-columns'); }
        if (i % 3 != 0) { addSidebarIcon($(o).children('ul').children('li'), 'fa-file-lines', 'far'); }
    });

    addSidebarIcon($('sidebar > div > div > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li'), 'fa-folder', 'far');

    $('sidebar > div > div > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul > li > ul').children('li').each(function (i, o) { 
        if (i % 3 == 0) { addSidebarIcon($(o).children('ul').children('li'), 'fa-columns'); }
        if (i % 3 != 0) { addSidebarIcon($(o).children('ul').children('li'), 'fa-file-lines', 'far'); }
    });

}

function enableSlider(box, slider_id, direction) {
    let block = $(box)[0];
    let slider = $(slider_id)[0];
    
    if ($(slider_id).hasClass('activated')) { return; }

    slider.onmousedown = function dragMouseDown(e) {
        let dragX = -1;
        if (direction == "vertical") {
            dragX = e.clientY;
        } else {
            dragX = e.clientX;
        }

        document.onmousemove = function onMouseMove(e) {
            if (direction == "vertical") {
                $('box.active editor').addClass('resized');
                block.style.height = block.offsetHeight + e.clientY - dragX + 'px';
                dragX = e.clientY;
            } else {
                block.style.width = block.offsetWidth + e.clientX - dragX + 'px';
                dragX = e.clientX;
            }
        }
        document.onmouseup = () => document.onmousemove = document.onmouseup = null;
    }

    $(slider_id).addClass('activated');
}

function enableResultsTableScroll() {
    $('box.active output results').off();
    if ($('box.active output results').hasClass('activated')) { return ; }
    $('box.active output results').on("scroll", function(e) {
        $('box.active output results thead').css('top', $(this).scrollTop().toString() + 'px');
    });
    $('box.active output results').addClass('activated');
}

function getCaretCharacterOffsetWithin(element) {
    var caretOffset = 0;
    var doc = element.ownerDocument || element.document;
    var win = doc.defaultView || doc.parentWindow;
    var sel;

    if (typeof win.getSelection != "undefined") {
        sel = win.getSelection();
        if (sel.rangeCount > 0) {
            var range = win.getSelection().getRangeAt(0);
            var preCaretRange = range.cloneRange();
            preCaretRange.selectNodeContents(element);
            preCaretRange.setEnd(range.endContainer, range.endOffset);
            caretOffset = preCaretRange.toString().length;
        }
    } else if ((sel = doc.selection) && (sel.type != "Control")) {
        var textRange = sel.createRange();
        var preCaretTextRange = doc.body.createTextRange();
        preCaretTextRange.moveToElementText(element);
        preCaretTextRange.setEndPoint("EndToEnd", textRange);
        caretOffset = preCaretTextRange.text.length;
    }

    return caretOffset
}

function setActiveConnection(conn_name) {
    $('#conn_name').text(conn_name);
}

function setActiveRole(role_name) {
    $('#role_name').text(role_name);
}

function updateStatusBar() {

    setActiveConnection($('tab.active').attr('conn-pref'));
    setActiveRole($('tab.active').attr('role-pref'));
    
    setTimeout(function() {
        if ($('box.active textarea.code').val() === undefined) { return; }

        /*
        if ($('box.active textarea.code').val().length > 0) {
            $('.btn-execute').prop('disabled', false);
        } else {
            $('.btn-execute').prop('disabled', true);
        }
        */

        if ($('box.active textarea.code').is(':focus')) {
            $('#pos').text('Pos 0');
            let p_loc = $('box.active textarea.code').prop("selectionEnd");
            if ($('box.active textarea.code').val().length == 0) { p_loc = 0; }
            if ((p_loc) && ($('box.active textarea.code').is(':focus'))) {
                $('#pos').text('Pos ' + p_loc);
            }

            $('#sel-pos').text('Sel 0 : 0');
            if (window.getSelection().toString()) {
                let s = window.getSelection().toString();
                let s_len = s.length;
                let s_lines = (s.indexOf("\n") !== -1) ? s.split('\n').length : 1;
                $('#sel-pos').text('Sel ' + s_len + ' : ' + s_lines);
            }
        }
}, 100);
}

function selectTo(table, cell, startCellIndex, startRowIndex) {
    var row = cell.parent();
    var cellIndex = cell.index();
    var rowIndex = row.index();

    var rowStart, rowEnd, cellStart, cellEnd;

    if (rowIndex < startRowIndex) {
        rowStart = rowIndex;
        rowEnd = startRowIndex;
    } else {
        rowStart = startRowIndex;
        rowEnd = rowIndex;
    }

    if (cellIndex < startCellIndex) {
        cellStart = cellIndex;
        cellEnd = startCellIndex;
    } else {
        cellStart = startCellIndex;
        cellEnd = cellIndex;
    }

    for (var i = rowStart; i <= rowEnd; i++) {
        var rowCells = table.find("tr").eq(i).find("td");
        for (var j = cellStart; j <= cellEnd; j++) {
            rowCells.eq(j - 1).addClass('selected');
        }
    }
}

function selectColumn(table, cell, startCellIndex) {
    var cellIndex = cell.index();
    var cellStart, cellEnd;

    if (cellIndex < startCellIndex) {
        cellStart = cellIndex;
        cellEnd = startCellIndex;
    } else {
        cellStart = startCellIndex;
        cellEnd = cellIndex;
    }

    for (var i = 0; i <= $('box.active results tbody').find('tr').length; i++) {
        var rowCells = table.find("tr").eq(i).find("td");
        for (var j = cellStart; j <= cellEnd; j++) {
            rowCells.eq(j - 1).addClass('selected');
        }
    }
}

function selectRow(table, cell, startRowIndex) {
    var rowIndex = cell.parent().index();
    var rowStart, rowEnd;

    if (rowIndex < startRowIndex) {
        rowStart = rowIndex;
        rowEnd = startRowIndex;
    } else {
        rowStart = startRowIndex;
        rowEnd = rowIndex;
    }

    for (var i = rowStart; i <= rowEnd; i++) {
        table.find("tr").eq(i).find("td").addClass('selected');
    }
}

function resetOutput() {
    $('box.active results thead th data').off();
    $('box.active results thead tr th').off();
    $('box.active results tbody td').off();

    $('box.active output controls button.copy').prop('disabled', true);
    $('box.active output controls button.download').prop('disabled', true);
    $('.btn-download').prop('disabled', true);
    $('.btn-refresh').prop('disabled', true);
    $('box.active output section.results toolbar button').prop('disabled', true);
    

    if ($('box.active results tbody tr').length > 0) {
        $('box.active output controls button.download').prop('disabled', false);
        $('box.active output section.results toolbar button').prop('disabled', false);
        $('.btn-download').prop('disabled', false);
        $('.btn-refresh').prop('disabled', false);
    }

    $('box.active results thead th data').mousedown(function(e) {
        let t = $(this);
        document.onmousemove = function onMouseMove(e) {
            $('box.active results tbody tr td:nth-child('+($(t).parent().index()+1)+') data').css('width', ($(t).width()+1) + 'px');
        };
        document.onmouseup = () => document.onmousemove = document.onmouseup = null;
    });

    var startCellIndex = null;
    var startRowIndex = null;
    var table = $('box.active results tbody');

    $('box.active results thead tr').find('th').first().click(function() {
        $('box.active output controls button.copy').prop('disabled', false);
        $('box.active results tbody tr td').addClass('selected');
    });

    table.find('td').mousedown(function(e) {
        isMouseDown = true;
        var cell = $(this);

        table.find('.selected').removeClass('selected');
        cell.addClass('selected');
        $('box.active output controls button.copy').prop('disabled', false);

        if (e.shiftKey) {
            selectTo(table, $(this), startCellIndex, startRowIndex);
        } else {
            cell.addClass("active");
            $('box.active output controls button.copy').prop('disabled', false);

            startCellIndex = cell.index();
            startRowIndex = cell.parent().index();
        }

        return false;
    })
    .mouseover(function() {
        if (!isMouseDown) return;
        $('box.active output controls button.copy').prop('disabled', false);

        table.find('.selected').removeClass('selected');
        selectTo(table, $(this), startCellIndex, startRowIndex);
    })
    .bind("selectstart", function() {
        return false;
    });

    var table2 = $('box.active results thead');

    table2.find('th handle').mousedown(function(e) {
        isMouseDown = true;
        var cell = $(this).parent();

        table.find('.selected').removeClass('selected');
        $('box.active output controls button.copy').prop('disabled', false);
        
        if (e.shiftKey) {
            selectColumn(table, $(this).parent(), startCellIndex);
        } else {
            startCellIndex = cell.index();
            startRowIndex = cell.parent().index();
            selectColumn(table, $(this).parent(), startCellIndex);
        }

        return false;
    })
    .mouseover(function() {
        if (!isMouseDown) return;
        $('box.active output controls button.copy').prop('disabled', false);
        
        table2.find('.selected').removeClass('selected');
        selectColumn(table, $(this).parent(), startCellIndex);
    })
    .bind("selectstart", function() {
        return false;
    });

    table.find('th').mousedown(function(e) {
        isMouseDown = true;
        var cell = $(this);

        table.find('.selected').removeClass('selected');
        $('box.active output controls button.copy').prop('disabled', false);
        
        if (e.shiftKey) {
            selectRow(table, $(this), startRowIndex);
        } else {
            startCellIndex = 1;
            startRowIndex = cell.parent().index();
            selectRow(table, $(this), startRowIndex);
        }

        return false;
    })
    .mouseover(function() {
        if (!isMouseDown) return;
        $('box.active output controls button.copy').prop('disabled', false);
        
        table.find('.selected').removeClass('selected');
        selectRow(table, $(this), startRowIndex);
    })
    .bind("selectstart", function() {
        return false;
    });

}

function copyData() {
    let headers = $('box.active output table thead tr').find('th');
    let rows = $('box.active output table tbody').find('tr');

    let headerLine = [];
    let data = []
    for (var x=0;x<rows.length;x++) {
        let rowCells = rows.eq(x).find('td');
        var hdr = [];
        var rowData = [];
        for (var j=0;j<rowCells.length;j++) {
            if (rowCells.eq(j).hasClass("selected")) {
                if (headerLine.length == 0) {
                    hdr.push(headers.eq(j+1).text());
                }

                rowData.push(rowCells.eq(j).text());

            }
        }

        if (hdr.length > headerLine.length) { headerLine = hdr.slice(); data.push(headerLine.join("\t")); }
        if (rowData.length > 0) { data.push(rowData.join("\t")); }
    }

    let outData = data.join("\n");
    navigator.clipboard
      .writeText(outData)
      .then(() => {
        //console.log("Data copied to clipboard")
      })
      .catch(() => {
        alert("Copy to Clipboard failed");
      });

}

function activateTab(el_id) {
    $('box').removeClass('active');
    $(el_id).addClass('active');

    $(el_id + ' editor').off();
    $(el_id + ' editor').click(function() { activeArea = "editor"; });

    $(el_id + ' output').off();
    $(el_id + ' output').click(function() { activeArea = "output"; });

    $(el_id + ' controls button.results').off();
    $(el_id + ' controls button.results').click(function() {
        $(el_id + ' section').hide();
        $(el_id + ' section.results').show();
        activeArea = "output";
        return false;
    });

    $(el_id + ' output controls button.code').off();
    $(el_id + ' output controls button.code').click(function() {
        $(el_id + ' output section').hide();
        $(el_id + ' output section.code').show();
        $(el_id + ' output controls button').removeClass('activated');
        $(this).addClass('activated');
        activeArea = "output";
        return false;
    });

    $(el_id + ' output controls button.server-output').off();
    $(el_id + ' output controls button.server-output').click(function() {
        $(el_id + ' output section').hide();
        $(el_id + ' output section.server-output').show();
        $(el_id + ' output controls button').removeClass('activated');
        $(this).addClass('activated');
        activeArea = "output";
        return false;
    });

    $(el_id + ' output controls button.results').off();
    $(el_id + ' output controls button.results').click(function() {
        $(el_id + ' output section').hide();
        $(el_id + ' output section.results').show();
        $(el_id + ' output controls button').removeClass('activated');
        $(this).addClass('activated');
        activeArea = "output";
        return false;
    });

    $(el_id + ' output controls button.copy').off();
    $(el_id + ' output controls button.copy').click(function() {
        copyData();
    });

    if ($('core').children('box.active').length == 0) {
        $($('core').children('box')[0]).addClass('active');
    }

    $('box.active textarea.code').off();

    if (!$('box.active').hasClass('activated')) {
        enableSlider('box.active editor', 'box.active .vslider', 'vertical');
        enableResultsTableScroll();

        $('box.active textarea.code').mouseup(function() { updateStatusBar(); });
        
        $('box.active textarea.code').keydown(function(e) { 
            if (!(escapeKey) && (e.keyCode == 9)) {
                e.preventDefault();

                let curPos = $('box.active textarea.code')[0].selectionStart; 
                let x = $('box.active textarea.code').val();
                let text_to_insert="    ";
                let curEnd = $('box.active textarea.code')[0].selectionEnd

                $('box.active textarea.code').val(x.slice(0,curPos)+text_to_insert+x.slice(curPos));
                $('box.active textarea.code')[0].selectionEnd = curEnd + text_to_insert.length;
            }
        });
        

        $('box.active textarea.code').keyup(function(e) { 
            if (!(escapeKey) && (e.keyCode == 9)) {
                e.preventDefault();
            }

            if (e.ctrlKey) {
                if (e.keyCode == 81) {
                    $('box.active .btn-execute').trigger('click');
                }
            }

            escapeKey = false;

            if (e.keyCode == 27) {
                escapeKey = true;
            }

            updateStatusBar(); 
        });
        
        $('box.active').addClass('activated');
        resetOutput();
    }

    $('box.active textarea.code').focus(function() {
        escapeKey = false;
    });

    if ($('box.active output tbody tr').length == 0) {
        $('box.active output controls button.copy').prop('disabled', true);
        $('box.active output controls button.download').prop('disabled', true);
        $('.btn-download').prop('disabled', true);
        $('.btn-refresh').prop('disabled', true);
        $('box.active output section.results toolbar button').prop('disabled', true);
    } else {
        $('box.active output controls button.copy').prop('disabled', false);
        $('box.active output controls button.download').prop('disabled', false);
        $('.btn-download').prop('disabled', false);
        $('.btn-refresh').prop('disabled', false);
        $('box.active output section.results toolbar button').prop('disabled', false);
    }
}

function loadResultData(box, data, append) {
    if (!append) {
        $(box).find('table thead tr').empty();
        $(box).find('table tbody').empty();
        $(box).find('table thead tr').append($('<th><spacer>&nbsp;</spacer></th>'));
        for (let i=0; i<data["columns"].length; i++) {
            let d = $('<data></data>');
            d.text(data["columns"][i]["name"]);
            let th = $('<th></th>');
            th.append(d);
            th.append($('<handle></handle>'));
            $('box.active table thead tr').append(th);
        }
    }

    for (let r=0; r<data["records"].length; r++) {
        let cnt = $(box).find('table tbody').find('tr').length;
        let tr = $("<tr></tr>");
        let d = $('<data></data>');
        d.text(cnt+1);
        let th = $('<th></th>');
        th.append(d);
        tr.append(th);

        for (let c=0; c<data["records"][r].length; c++) {
            let d = $('<data></data>');
            d.text(data["records"][r][c]);
            d.addClass((data["columns"][c]["type"]+"").toLowerCase());
            let td = $('<td></td>');
            td.append(d);
            tr.append(td);
        }            

        $(box).find('table tbody').append(tr);

    }

    let output_text = data["output"].join('\n');
    if (output_text.length <= 0) { output_text = "Request completed successfully."; }
    $(box).find('.server-output-text').text(output_text);
    if (data["count"] < 0) {
        $(box).find('.server-output').trigger('click');
    }

    if (("error" in data) && (data["error"] != null) && (data["error"] != "")) {
        $(box).find('.error-msg').text(data["error"]);
        $(box).find('controls .results').trigger('click');
        $(box).find('.error-msg').show();
    }

    resetOutput();
}

function selectText(node) {
    
    if (document.body.createTextRange) {
        const range = document.body.createTextRange();
        range.moveToElementText(node);
        range.select();
    } else if (window.getSelection) {
        const selection = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(node);
        selection.removeAllRanges();
        selection.addRange(range);
    } else {
        console.warn("Could not select text in node: Unsupported browser.");
    }
}

function activateSidebar() {
    $('sidebar li > button').click(function() {
        if ($(this).parent().children('ul').css('display') == "block") {
            $(this).find('i.fa-caret-down').addClass('fa-caret-right').removeClass('fa-caret-down');
            $(this).parent().children('ul').hide();
        } else {
            $(this).find('i.fa-caret-right').addClass('fa-caret-down').removeClass('fa-caret-right');
            $(this).parent().children('ul').show();
        }
    });
}

function getSql() {
    if (window.getSelection().toString()) {
        let s = window.getSelection().toString();
        return s;
    } else {
        return $('box.active editor .code').val();
    }
}

function executeQuery() {
    let sql_statement = getSql();

    if (sql_statement.trim() == "") {
        return;
    }
    
    let conn_name = $('tab.active').attr('conn-pref');
    let role_name = $('tab.active').attr('role-pref');
    let db_name = $('tab.active').attr('db-pref');

    let box = $('box.active');
    let record_count = 0;
    let start_time = new Date().getTime();

    var xhr = $.ajax({
        url: api_url,
        dataType: "json",
        method: "POST",
        data: JSON.stringify({ command: "query", connection: conn_name, role: role_name, db: db_name, sql: sql_statement }),
        contentType: "application/json",
        timeout: 0,
        beforeSend: function() {
            $(box).find('loading').show();

            $(box).find('controls button').prop('disabled', true);
            if (!$(box).find('editor').hasClass('resized')) {
                $(box).find('editor').css('height', "50vh");
            }

            $(box).find('button.btn-cancel-request').click(function() {
                xhr.abort();
            });
        },
        success: function(data) {
            $(box).find('table thead tr').empty();
            $(box).find('table tbody tr').empty();
            $(box).find('controls .results').trigger('click');
            
            $(box).find('.error-msg').hide();
            $(box).find('output section.code text.sql-text').text(sql_statement.toString().trim());
            
            loadResultData(box, data);

            $(box).find('controls button').prop('disabled', false);
            $(box).find('controls button.copy').prop('disabled', true);
            $(box).find('loading button').off();
            $(box).find('loading').hide();

            record_count = data["count"];

            activeArea = "editor";
            $(box).find('editor .code').focus();
        },
        error: function(jqXHR, textStatus, errorThrown) {
            // textStatus = "timeout", "error", "abort", and "parsererror"
            
            if (textStatus == "abort") {
                $(box).find('.error-msg').text('Request canceled.');
            } else if (textStatus == "timeout") {
                $(box).find('.error-msg').text('Request timed out.');
                $(box).find('controls .results').trigger('click');
                $(box).find('.error-msg').show();
            } else {
                $(box).find('.error-msg').text('An error occurred while trying to connect to the service.');
                $(box).find('controls .results').trigger('click');
                $(box).find('.error-msg').show();
            }
            
            $(box).find('controls button').prop('disabled', false);
            $(box).find('controls button.copy').prop('disabled', true);
            $(box).find('loading button').off();
            $(box).find('loading').hide();
        },
        complete: function() {
            let end_time = new Date().getTime();
            let run_time = Math.round((end_time - start_time) / 10);

            if (record_count >= 0) {
                $('statusbar #time').text((record_count)+' records - '+(run_time / 100)+' seconds')
            } else {
                $('statusbar #time').text((run_time / 100)+' seconds')
            }
        }
    })
}

function addTab(conn_name, role_name, tab_name, db_name) {
    if (!tab_name) {
        tab_name = $('#conn_name').text();
    }

    if (!conn_name) {
        conn_name = $('#conn_name').text();
    }

    if (!role_name) {
        role_name = $('#role_name').text();
    }

    if (!db_name) {
        db_name = $('#database_name').text();
    }

    if ((conn_name == "") || (role_name == "") || (db_name.trim() == "")) {
        $('.btn-add-tab').trigger('click');
        return;
    }

    let data = $('#tab-template').html();

    let tab_id = "tab" + tabIncrement;
    tabIncrement++;
    let tab_link = $('<tab target="#'+tab_id + '"><span class="tab_name"></span><button><i class="fas fa-times"></i></button></tab>');
    $(tab_link).find('.tab_name').text(tab_name);
    $(tab_link).attr('role-pref', role_name);
    $(tab_link).attr('conn-pref', conn_name);
    $(tab_link).attr('db-pref', db_name);

    $('tabs item#tabs').prepend(tab_link);

    $(tab_link).click(function() {
        $('#role_name').text($(this).attr('role-pref'));
        $('#conn_name').text($(this).attr('conn-pref'));
        activateTab($(this).attr('target'));
        $('tab').removeClass('active');
        $(this).addClass('active');

        let conn_name = $(this).attr('conn-pref');
        let role_name = $(this).attr('role-pref');
        let db_name = $(this).attr('db-pref');
    
        if ((conn_name !== undefined) && (conn_name != "")) {
            $('#conn_name').text(conn_name);
        }
        
        if ((role_name !== undefined) && (role_name != "")) {
            $('#role_name').text(role_name);
        }
    
        if ((db_name !== undefined) && (db_name != "")) {
            $('#database_name').text(db_name);
        }
    
    
    });

    $(tab_link).find('button').click(function() {
        $('core.tabs '+$(this).parent().attr('target')).remove();
        let isActive = false;
        if ($(this).parent().hasClass('active')) { isActive = true; }
        let idx = $(this).parent().index();
        $($(this).parent().attr('target')).remove();
        $(this).parent().remove();

        if ($('tabs item#tabs').find('tab').length == 0) {
            addTab();
            return;
        }

        if (isActive) {
            if ($('tabs item#tabs').find('tab').length > idx) {
                $('tabs item#tabs').find('tab').eq(idx).trigger('click');
                return;
            }

            if ($('tabs item#tabs').find('tab').length == idx) {
                $('tabs item#tabs').find('tab').eq(idx - 1).trigger('click');
                return;
            }
        }
    });

    let tab_info = $(data);
    $(tab_info).prop('id', tab_id);

    $('core.tabs').append(tab_info);
    $(tab_link).trigger('click');

    $('box.active .btn-tab-settings').click(function(event) {
        var ul = $('box.active .tab-settings ul');
        ul.empty();

        let conn_name = $('tab.active').attr('conn-pref');
        let role_name = $('tab.active').attr('role-pref');
        let db_name = $('tab.active').attr('db-pref');

        let li = $('<li></li>');
        li.html('<span class="name">Connection</span><span class="value"></span>');
        li.find('.value').text(conn_name)
        li.appendTo(ul);

        li = $('<li></li>');
        li.html('<span class="name">Role</span><span class="value"></span>');
        li.find('.value').text(role_name)
        li.appendTo(ul);

        li = $('<li></li>');
        li.html('<span class="name">Database</span><span class="value"></span>');
        li.find('.value').text(db_name)
        li.appendTo(ul);

        $('box.active .tab-settings').show();
        activeArea = "editor";
        return false;

    });

    $('box.active .tab-settings button').click(function() {
        activeArea = "editor";
        $('box.active .tab-settings').hide();
    });

    $('box.active .btn-execute').click(function() { executeQuery(); $('box.active editor .code').focus(); activeArea = "editor"; });
    
    $('box.active editor textarea.code').focus();
    activeArea = "editor";

    $('box.active editor').css('height', "100vh");
    updateStatusBar();
    doPing();

}

function getSidebarDetails(obj, data) {
    if (data === undefined) { data = {}; }

    let this_obj = $(obj);
    while (true) {
        if (($(this_obj).prop('tagName').toLowerCase() != 'ul') && $(this_obj).prop('tagName').toLowerCase() != 'li') {
            break;
        }

        if ($(this_obj).prop('tagName').toLowerCase() == 'li') {
            let section = $(this_obj).children('button').attr('sidebar-section');
            let obj_nm = $(this_obj).children('button').children('span.object').text();
            if (!data[section]) {
                data[section] = obj_nm;
            }
        }

        this_obj = $(this_obj).parent();
    }

    return data;
}

function loadSidebarSection(obj) {

    $('.sidebar-context-menu').hide();
    let section = $(obj).children('button').attr('sidebar-section');
    let request = { command: "meta", "request_type": section };

    let this_obj = $(obj);
    /*
    while (true) {
        if (($(this_obj).prop('tagName').toLowerCase() != 'ul') && $(this_obj).prop('tagName').toLowerCase() != 'li') {
            break;
        }

        if ($(this_obj).prop('tagName').toLowerCase() == 'li') {
            let section = $(this_obj).children('button').attr('sidebar-section');
            let obj_nm = $(this_obj).children('button').children('span.object').text();
            request[section] = obj_nm;
        }

        this_obj = $(this_obj).parent();
    }
    */
    request = getSidebarDetails(this_obj, request);

    if ($(obj).children('button').children('i').eq(0).hasClass('fa-caret-down')) { 
        $(obj).children('button').children('i').eq(0).removeClass('fa-caret-down');
        $(obj).children('button').children('i').eq(0).addClass('fa-caret-right');
        $(obj).children('ul').hide();

        return;
    }

    if ($(obj).hasClass('section-loaded')) {
        $(obj).children('button').children('i').eq(0).removeClass('fa-caret-right');
        $(obj).children('button').children('i').eq(0).addClass('fa-caret-down');
        $(obj).children('ul').show();

        return;
    }

    $(obj).find('ul').empty();

    $.ajax({
        url: api_url,
        method: "POST",
        contentType: "application/json",
        dataType: "json",
        data: JSON.stringify(request),
        beforeSend: function() {
            if ($(obj).children('button').children('i').eq(0).hasClass('no-child')) { return; }
            $(obj).children('button').children('i').eq(0).removeClass('fa-caret-right');
            $(obj).children('button').children('i').eq(0).addClass('fa-spinner');
            $(obj).children('button').children('i').eq(0).addClass('fa-spin');
        },
        success: function(data) {
            if (!data["ok"]) {
                doLogout();
                return;
            }

            if (data["error"]) {
                alert(data["error"]);
            }

            if (Array.isArray(data["data"])) {
                for (let i=0; i<data["data"].length; i++) {
                    let obj_title = (data["title"] !== undefined) ? data["title"] : data["data"][i];

                    let entry = $('<li></li>');
                    entry.prop('id', uuidv4());
                    entry.html('<button><i class="caret fas fa-fw fa-caret-right"></i><i class="icon fas fa-fw"></i><span class="object"></span><span class="extra"></span></button><ul></ul>');
                    $(entry).children('button').attr('sidebar-section', data["type"]);

                    if ((typeof data["data"][i] === 'object') && (!Array.isArray(data["data"][i]))) {
                        entry.find('span.object').text(data["data"][i]["name"]);
                        entry.find('span.extra').text(data["data"][i]["extra"]);
                    } else {
                        entry.find('span.object').text(data["data"][i])
                    }

                    entry.find('i').first().click(function() {
                        loadSidebarSection($(this).parent().parent());
                        return false;
                    });
                    entry.find('button').on('contextmenu', function(event) { 
                        $('.sidebar-context-menu').hide();

                        let sidebar_data = getSidebarDetails($(this).parent());

                        $('.sidebar-context-menu').attr('object-id', $(this).parent().prop('id'));
                        $('.sidebar-context-menu').attr('object-type', $(this).attr('sidebar-section'));
                        $('.sidebar-context-menu').attr('object-name', $(this).find('span.object').text());
                        let table_name = "";
                        if ("table" in sidebar_data) {
                            table_name = sidebar_data["table"];
                        }
                        $('.sidebar-context-menu').attr('object-parent', table_name);
                        $('.sidebar-context-menu').attr('object-schema', sidebar_data["schema"]);
                        $('.sidebar-context-menu').attr('object-server', sidebar_data["server"]);
                        $('.sidebar-context-menu').attr('object-db', sidebar_data["database"]);

                        $('.sidebar-context-menu').css(
                            {
                                display: "block",
                                top: event.pageY + "px",
                                left: event.pageX + "px"
                            }
                        );

                        $('.sidebar-context-menu').addClass('hide-options');
                        if (["view", "mat_view", "index", "policy", "trigger", "sequence", "constraint", "procedure", "function"].includes($(this).attr('sidebar-section'))) {
                            $('.sidebar-context-menu').removeClass('hide-options');
                        }

                        $('.sidebar-context-menu').addClass('hide-perms');
                        if (["schema", "table", "view", "mat_view", "sequence", "procedure", "function", "partition"].includes($(this).attr('sidebar-section'))) {
                            $('.sidebar-context-menu').removeClass('hide-perms');
                        }
        
                    });

                    entry.appendTo($(obj).children('ul'));
                }
            } else {
                $(obj).children('button').children('i').eq(0).removeClass('fa-caret-right');
                $(obj).children('button').children('i').eq(0).removeClass('fa-caret-down');
                $(obj).children('button').children('i').eq(0).removeClass('fa-spinner');
                $(obj).children('button').children('i').eq(0).removeClass('fa-spin');
                $(obj).children('button').children('i').eq(0).addClass('no-child');
            }

            fixIconClasses();
            $(obj).addClass('section-loaded');
        },
        complete: function() {
            if ($(obj).children('button').children('i').eq(0).hasClass('no-child')) { return; }
            $(obj).children('button').children('i').eq(0).removeClass('fa-spinner');
            $(obj).children('button').children('i').eq(0).removeClass('fa-spin');
            $(obj).children('button').children('i').eq(0).addClass('fa-caret-down');
            $(obj).children('ul').show();
        }
    })
    
    fixIconClasses();
}

function loadConnectionList(connections) {
    if (connections === undefined) { return; }

    let connections_list = Object.keys(connections);
    $('sidebar').addClass('connections-loaded');

    $('#select-connection > ul').empty();

    if (connections_list.length > 0)  {
        connections_list.sort();

        $('sidebar > div > div > ul').empty();
        let x = 1;
        for (let i=0; i<connections_list.length; i++) {
            let entry = $('<li></li>');
            entry.prop('id', uuidv4());
            entry.html('<button sidebar-section="server"><i class="caret fas fa-fw fa-caret-right"></i><i class="icon fas fa-fw"></i><span class="object"></span><span class="extra"></span></button><ul></ul>');
            entry.find('span.object').text(connections_list[i]);
            entry.find('i').first().click(function() {
                loadSidebarSection($(this).parent().parent());
            });
            entry.find('button').click(function() { $('.sidebar-context-menu').hide(); });
            entry.appendTo($('sidebar > div > div > ul'));

            entry.find('button').on('contextmenu', function(event) { 
                $('.sidebar-context-menu').hide();

                let sidebar_data = getSidebarDetails($(this).parent());

                $('.sidebar-context-menu').attr('object-type', $(this).attr('sidebar-section'));
                $('.sidebar-context-menu').attr('object-name', $(this).find('span.object').text());
                let table_name = "";
                if ("table" in sidebar_data) {
                    table_name = sidebar_data["table"];
                }

                $('.sidebar-context-menu').attr('object-id', $(this).parent().prop('id'));
                $('.sidebar-context-menu').attr('object-parent', table_name);
                $('.sidebar-context-menu').attr('object-schema', sidebar_data["schema"]);
                $('.sidebar-context-menu').attr('object-server', sidebar_data["server"]);
                $('.sidebar-context-menu').attr('object-db', sidebar_data["database"]);

                $('.sidebar-context-menu').css(
                    {
                        display: "block",
                        top: event.pageY + "px",
                        left: event.pageX + "px"
                    }
                );

                $('.sidebar-context-menu').addClass('hide-options');
                if (["view", "mat_view", "index", "policy", "trigger", "sequence", "constraint", "procedure", "function"].includes($(this).attr('sidebar-section'))) {
                    $('.sidebar-context-menu').removeClass('hide-options');
                }

                $('.sidebar-context-menu').addClass('hide-perms');
                if (["schema", "table", "view", "mat_view", "sequence", "procedure", "function", "partition"].includes($(this).attr('sidebar-section'))) {
                    $('.sidebar-context-menu').removeClass('hide-perms');
                }
            });

            let chooser = $('<li></li>');
            chooser.html('<span></span><ul></ul>');
            chooser.find('span').text(connections_list[i]);
            let role_names = connections[connections_list[i]];
            role_names.sort();

            role_names.forEach(function(value) {
                
                let li = $('<li></li>');
                li.html('<input type="radio" name="role-chooser" id="role-chooser'+x+'" value="#role-chooser'+x+'"><label for="role-chooser'+x+'"></label>');
                li.find('input').attr('role-name', value);
                li.find('input').attr('conn-name', connections_list[i]);
                li.find('label').text(value);
                li.appendTo(chooser.find('ul'));
                x++;
            });

            chooser.appendTo($('#select-connection > ul'));
        }

        $('#conn_name').text(connections_list[0]);
        $('#role_name').text(connections[connections_list[0]][0]);

    }

    fixIconClasses();
}

function loadApp(settings) {
    let connections = settings["connections"];

    $('overlay#login').hide();
    $('body > toolbar').show();
    $('body > statusbar').show();
    $('body > page').css('display', 'flex');

    if (($('tabs').find('tab') === undefined) || ($('#tabs').find('tab').length == 0)) {
        $('overlay#connection-chooser').attr('target', 'new-tab');
    }

    if ((connections !== undefined) && (!$('sidebar').hasClass('connections-loaded'))) {
        loadConnectionList(connections);
        if (settings["default_dbs"][$('#conn_name').text()] !== undefined) {
            let db_name = settings["default_dbs"][$('#conn_name').text()];
            if ((db_name != null) && (db_name != "")) {
                $('#database_name').text(db_name);
            } else {
                if (($('tabs').find('tab') !== undefined) && ($('#tabs').find('tab').length > 0)) {
                    $('tab.active').trigger('click');
                } else {
                    $('#database_name').html('&nbsp;');
                }
            }
        }
    }

    if (($('tabs').find('tab') === undefined) || ($('#tabs').find('tab').length == 0)) {
        if (connections !== undefined) {
            addTab();
        } else {
            alert("No connections found.")
        }
    }
}

function doLogout() {

    $.ajax({
        url: api_url,
        method: "POST",
        dataType: "JSON",
        contentType: "application/json",
        data: JSON.stringify({ "command": "logout" }),
        success: function(data) {
            clearTimeout(auto_timer);
            $('overlay#login').show();
            $('page').attr('logged-in', 'no');
            $('sidebar').removeClass('connections-loaded');
            $('sidebar > div > div > ul').empty();
        }
    });

}

function updateTimer() {
    auto_timer = setTimeout(function() {
        checkLogin();
    }, 60000);
}

function checkLogin() {

    $.ajax({
        url: api_url,
        method: "POST",
        dataType: "JSON",
        contentType: "applicaton/JSON",
        data: JSON.stringify({ command: "auth" }),
        success: function(data) {
            if (!data["ok"]) {
                doLogout();
            } else {
                $('page').attr('logged-in', 'yes');
                if (data["auth_type"] == "config") { 
                    $('.btn-logout').hide();
                    $('.btn-role-name').hide();
                }
                loadApp(data);
                updateTimer();
            }
        }
    });

}

function doLogin() {

    $.ajax({
        url: api_url,
        method: "POST",
        data: JSON.stringify({
            command: "login",
            username: $('input#username').val(),
            password: $('input#password').val()
        }),
        contentType: "application/json",
        dataType: "JSON",
        beforeSend: function() {
            $('button#login').prop('disabled', true);
        },
        success: function(data) {
            if (data["ok"]) {
                $('page').attr('logged-in', 'yes');

                if ($('page').attr('logged-in-user') != $('input#username').val()) {
                    $('sidebar > div > div > ul').empty();
                    $('sidebar').removeClass('connections-loaded');
                    $('tab button').each(function(i,o) { $(o).trigger('click'); });
                }

                $('page').attr('logged-in-user', $('input#username').val());

                loadApp(data);

                updateTimer();
            }
        },
        complete: function() {
            $('button#login').prop('disabled', false);
            $('input#password').val('');
        }
    });

    $('overlay#login').show();

}

function doPing() {
    $.ajax({
        url: api_url,
        method: "POST",
        dataType: "json",
        contentType: "application/json",
        data: JSON.stringify({ command: "ping" }),
        success: function(data) {
            if (!data["ok"]) {
                doLogout();
            }
        }
    })
}

function openSelectDatabase(conn_name) {
    $.ajax({
        url: api_url,
        method: "POST",
        contentType: "application/json",
        dataType: "json",
        data: JSON.stringify({command: "meta", request_type: "server", server: conn_name}),
        success: function(data) {

            if (!data["ok"]) {
                doLogout();
                return;
            }

            var ul = $('overlay#database-chooser .message > div > ul');
            ul.empty();

            var i = 1;
            data["data"].forEach(function(o) {
                let li = $('<li></li>');
                li.html('<input type="radio" name="db-chooser" id="db-chooser'+i+'" value="#db-chooser'+i+'" /><label for="db-chooser'+i+'"></label>');
                li.find('input').attr('db-name', o);
                li.find('label').text(o);
                li.appendTo(ul);
                i++;
            });

            $('overlay#connection-chooser').fadeOut('fast');

            if ($('#select-database > ul').children('li').length == 1) {
                $('#db-chooser1').prop('checked', true);
                $('#btn-select-database').trigger('click');
            } else {
                $('overlay#database-chooser').fadeIn('fast');
            }
        }
    });
}

$(document).ready(function() {

    $('toolbar').bind("contextmenu", function(event) {
        event.preventDefault();
    });

    $('statusbar').bind("contextmenu", function(event) {
        event.preventDefault();
    });

    $('slider').bind("contextmenu", function(event) {
        event.preventDefault();
    });

    $('tabs').bind("contextmenu", function(event) {
        event.preventDefault();
    });

    $('sidebar').bind("contextmenu", function(event) {
        event.preventDefault();
    });

    $(document).keydown(function (e) {
        if (e.which === 27) {
            $('.tab-settings').hide();
            $('overlay.chooser').hide();
            $('button').blur();
        }
    });

    window.onbeforeunload = () => {
        if ($('page').attr('logged-in') == "yes") {
            if (confirm('Are you sure you want to leave?')) { return true; } else { return false; }
        }
    }

    enableSlider('sidebar', '.hslider', 'horizontal');

    $('.btn-refresh-sidebar').click(function() {
        $('sidebar > div > div > ul > li').removeClass('section-loaded');
        $('sidebar > div > div > ul > li > ul').hide();
        $('sidebar > div > div > ul').children('li').each(function(i,o) {
            $(o).children('button').children('i').eq(0).removeClass('fa-spin');
            $(o).children('button').children('i').eq(0).removeClass('fa-spinner');
            $(o).children('button').children('i').eq(0).removeClass('fa-caret-down');
            $(o).children('button').children('i').eq(0).removeClass('no-child');
            $(o).children('button').children('i').eq(0).addClass('fa-caret-right');
        });
    });

    $(document).mouseup(function() { isMouseDown=false; updateStatusBar(); });

    $('sidebar').click(function() { activeArea = "sidebar"; });
    $('statusbar').click(function() { activeArea = "statusbar"; });
    $('toolbar').click(function() { activeArea = "toolbar"; });

    $(document).keydown(function(e) { 
        if ((activeArea == "output") && (e.ctrlKey) && (e.keyCode == 65)) {
            if ($('box.active section.results').css('display') == "block") {
                $('box.active section.results table td').addClass("selected");
            }

            if ($('box.active section.code').css('display') == "block") {
                selectText($('box.active section.code text').first()[0]);
            }

            if ($('box.active section.server-output').css('display') == "block") {
                selectText($('box.active section.server-output text').first()[0]);
            }
            
            e.preventDefault();
            return false;
        } else {
            if ((activeArea == "output") && (e.ctrlKey) && (e.keyCode == 67)) {
                copyData();
            }
        }
    });

    $('tabs > selector button').click(function() {
        if ($('tabs > selector ul').css('display') == "block") {
            $('tabs > selector button').removeClass('activated');
            $('tabs > selector ul').hide();
            return false;
        } else {
            let ul = $('tabs > selector ul');
            ul.empty();

            $('tabs item#tabs tab').each(function(i,o) {
                let li = $('<li></li>');
                li.text($(o).find('span').text());
                li.attr('idx', i);
                li.click(function() { var t = $('tabs item#tabs').find('tab').eq($(this).attr('idx')); $('tabs item#tabs').prepend(t); t.trigger('click'); });
                ul.append(li);
            });

            $('tabs > selector button').addClass('activated');
            $('tabs > selector ul').show();
            return false;
        }
    });

    activateSidebar();

    $(document).click(function() {
        if ($('tabs > selector ul').css('display') == "block") {
            $('tabs > selector button').removeClass('activated');
            $('tabs > selector ul').hide();
        }

        $('box.active .tab-settings').hide();
        $('.sidebar-context-menu').hide();
    });

    $('.btn-execute').click(function() {
        executeQuery();
        $('box.active editor .code').focus(); 
        activeArea = "editor";
    });

    $('.btn-chooser-close').click(function() {
        let target_id = $(this).attr('overlay-target');
        $(target_id).fadeOut('fast');
        $('.btn-add-tab').blur();
    });

    $('#btn-select-connection').click(function() {

        let chosen_id = $('input[name="role-chooser"]:checked').val();
        if ((chosen_id === undefined) || (chosen_id == null) || (chosen_id == "")) {
            alert('Please choose a connection.');
            return;
        }
        
        let conn_name = $(chosen_id).attr('conn-name');
        openSelectDatabase(conn_name);
        
    });

    $('#btn-select-database').click(function() {

        let chosen_id = $('input[name="role-chooser"]:checked').val();
        $('#conn_name').text($(chosen_id).attr('conn-name'));
        $('#role_name').text($(chosen_id).attr('role-name'));

        let db_id = $('input[name="db-chooser"]:checked').val();
        $('#database_name').text($(db_id).attr('db-name'));

        if ($('overlay#connection-chooser').attr('target') == "new-tab") {
            addTab($(chosen_id).attr('conn-name'), $(chosen_id).attr('role-name'), $(chosen_id).attr('conn-name'), $(db_id).attr('db-name'));
        } else {
            // Update tab connection
            let tab_link = $('tab.active');
            $(tab_link).find('.tab_name').text($(chosen_id).attr('conn-name'));
            $(tab_link).attr('role-pref', $(chosen_id).attr('role-name'));
            $(tab_link).attr('conn-pref', $(chosen_id).attr('conn-name'));
            $(tab_link).attr('db-pref', $(db_id).attr('db-name'));
            doPing();
        }

        $('overlay#connection-chooser').attr('target', '');
        $('.btn-chooser-close').trigger("click");
        $(chosen_id).prop('checked', false);
        $(db_id).prop('checked', false);
    });

    $('.btn-add-tab').click(function() {

        $('overlay#connection-chooser').attr('target', 'new-tab');

        if ($('#select-connection > ul').children('li').length == 1) {
            $('#role-chooser1').prop('checked', true);
            $('#btn-select-connection').trigger('click');
            return;
        }

        $('overlay#connection-chooser').fadeIn('fast');
    });

    $('.btn-role-name').click(function() {
        $('overlay#connection-chooser').attr('target', 'update-connection');
        $('overlay#connection-chooser').fadeIn('fast');
    });

    fixIconClasses();

    $('.btn-logout').click(function() {
        doLogout();
    });

    $('overlay#login button').click(function() {
        doLogin();
        return false;
    });

    $('.btn-db-name').click(function() {
        let conn_name = $('#conn_name').text();
        openSelectDatabase(conn_name);
    });

    checkLogin();

    $('#btn-refresh-item').click(function() {
        $('.sidebar-context-menu').hide();
        let object_id = $('.sidebar-context-menu').attr('object-id');
        if (object_id) {
            $('#' + object_id).removeClass('section-loaded');
            $('#' + object_id + ' ul').find('li').remove();
            $('#' + object_id + ' button').find('i').removeClass('fa-caret-down');
            $('#' + object_id + ' button').find('i').removeClass('fa-caret-up');
            $('#' + object_id + ' button').find('i').first().trigger('click');
        }
    });

    $('#btn-generate-ddl').click(function() {
        
        let obj_type = $('.sidebar-context-menu').attr('object-type');
        let obj_name = $('.sidebar-context-menu').attr('object-name');
        let obj_schema = $('.sidebar-context-menu').attr('object-schema');
        let obj_server = $('.sidebar-context-menu').attr('object-server');
        let obj_parent = $('.sidebar-context-menu').attr('object-parent');
        let obj_db = $('.sidebar-context-menu').attr('object-db');

        $.ajax({
            url: api_url,
            method: "POST",
            dataType: "json",
            contentType: "application/json",
            data: JSON.stringify({
                command: "ddl",
                type: obj_type,
                server: obj_server,
                database: obj_db,
                schema: obj_schema,
                name: obj_name,
                parent: obj_parent
            }),
            success: function(data) {
                if (!data["ok"]) {
                    doLogout();
                    return;
                }

                $('#ddl-statement').text(data["ddl"]);
                $('#ddl-data .message').removeClass('active');
                $('#ddl-data').show();
            }
        });

        $('.sidebar-context-menu').hide();
        return false;
    });

    $('.btn-copy-code').click(function() {
        let tgt = $(this).attr('rel');
        $('#ddl-data .message').addClass('active');

        let outData = $(tgt).text();
        navigator.clipboard
            .writeText(outData)
            .then(() => {
                //console.log("Data copied to clipboard")
            })
            .catch(() => {
                alert("Copy to Clipboard failed");
            });

        setTimeout(function() { $('#ddl-data .message').removeClass('active'); }, 150);

    });

});