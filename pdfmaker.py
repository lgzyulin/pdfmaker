#本代码仅针对一个a文件夹中有n个小文件夹，小文件夹中全是图片的情境。它能够把a文件夹中所有图片转化为n个pdf文件，文件以小文件夹的名字命名。
#如有不当请指出，其实是我下载本子合集时出问题，自己随便搞的，大佬不喜勿喷。
#本人电脑i5-13500尚有卡死内存溢出风险，建议每个小文件夹图片不超过1G。
import os
import gc
from PIL import Image
from concurrent.futures import ProcessPoolExecutor
import logging
import psutil

logging.basicConfig(filename='conversion.log', level=logging.INFO)

def calculate_chunk_size(folder_path, max_mem_mb=900):
    """动态计算分块大小[1,2]"""
    total_size = sum(os.path.getsize(os.path.join(folder_path, f)) 
                    for f in os.listdir(folder_path) 
                    if f.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.tiff')))
    avg_size_mb = total_size / len(os.listdir(folder_path)) / 1024 / 1024 if os.listdir(folder_path) else 0
    return max(1, int(max_mem_mb / avg_size_mb)) if avg_size_mb > 0 else 10

def process_folder(folder_path):
    try:
        image_files = [
            os.path.join(folder_path, f) 
            for f in os.listdir(folder_path)
            if f.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.tiff'))
        ]
        
        if not image_files:
            logging.warning(f"空文件夹：{folder_path}")
            return

        image_files.sort()
        chunk_size = calculate_chunk_size(folder_path)
        temp_dir = os.path.join(folder_path, "_temp_pdf")
        os.makedirs(temp_dir, exist_ok=True)

        # 分块处理机制[2,7]
        for chunk_idx, i in enumerate(range(0, len(image_files), chunk_size)):
            images = []
            for img_path in image_files[i:i+chunk_size]:
                try:
                    img = Image.open(img_path).convert('RGB')
                    img.thumbnail((2480, 3508))  # A4尺寸优化[1]
                    images.append(img)
                except Exception as e:
                    logging.error(f"图片处理失败：{img_path} - {str(e)}")
            
            if images:
                temp_pdf = os.path.join(temp_dir, f"part_{chunk_idx:04d}.pdf")
                save_args = {
                    'save_all': True,
                    'append_images': images[1:],
                    'resolution': 100,
                    'optimize': True  # 启用压缩[2]
                }
                images[0].save(temp_pdf,**save_args)
                
                # 强制释放内存[7]
                for img in images:
                    img.close()
                del images
                gc.collect()

        # 合并分块PDF[3]
        from PyPDF2 import PdfMerger
        merger = PdfMerger()
        for part in sorted(os.listdir(temp_dir)):
            merger.append(os.path.join(temp_dir, part))
        pdf_path = os.path.join(os.path.dirname(folder_path), f"{os.path.basename(folder_path)}.pdf")
        merger.write(pdf_path)
        merger.close()

        # 清理临时文件
        for f in os.listdir(temp_dir):
            os.remove(os.path.join(temp_dir, f))
        os.rmdir(temp_dir)
        
        logging.info(f"成功生成：{pdf_path}")

    except Exception as e:
        logging.critical(f"处理失败：{folder_path} - {str(e)}")

def batch_convert_folders_to_pdf(root_folder):  # 取消注释
    """多进程处理主函数"""
    folder_list = [
        os.path.join(root_folder, f) 
        for f in os.listdir(root_folder) 
        if os.path.isdir(os.path.join(root_folder, f))
    ]
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(process_folder, folder_list)
        
if __name__ == "__main__":
    #引号内填写a文件夹路径
    main_folder = r"C:\a"
    
    # 内存监控（可选）
    import tracemalloc
    tracemalloc.start()
    
    batch_convert_folders_to_pdf(main_folder)
