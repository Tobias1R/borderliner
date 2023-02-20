import argparse
import os

class BorderlinerCommand:
    merged_dict = {}
    def __init__(self, name,*args,**kwargs):
        self.class_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates'))
        self.template_dirs = [self.class_file_path]
        self.args = args
        self.name = name
        self.parser = argparse.ArgumentParser()
        # self.parser.add_argument('--template_extra_path', type=str, help='The template to use',default=None)
        # self.parser.add_argument('--template', type=str, help='The template to use')
        # self.parser.add_argument('--target', type=str, help='The target to use')
        # self.parser.add_argument('--source', type=str, help='The source to use')
        # self.parser.add_argument('--pipeline_name', type=str, help='The pipeline name to use')
        self.parser.add_argument('args', nargs=argparse.REMAINDER)
        
        self.args = self.parser.parse_args(args)
        print(self.args)
        self.extra_parser = argparse.ArgumentParser()

        self.extra_args = {}
        for arg in self.args.args:
            if '=' in arg:
                key,value = arg.split('=') 
                self.extra_args[key]=value
        self.extra_args["config_yml_file"] = self.get_argument('pipeline_name')+"_config.yml"
        if self.get_argument('template_extra_path'):
            self.template_dirs.extend(self.get_argument('template_extra_path').split(','))
        print(f"Template directories: {self.template_dirs}")

        
        
        # # Create a separate ArgumentParser instance for parsing the extra arguments
        # self.extra_parser = argparse.ArgumentParser()
        # for arg in self.extra_args:
        #     if "=" in arg:
        #         key, value = arg.split("=", 1)
        #         self.extra_parser.add_argument(key.replace('--',''), default=value)
        #     else:
        #         self.extra_parser.add_argument(arg,default='<YOUR_VALUE>')
        # self.extra_args_parsed = self.extra_parser.parse_args(self.extra_args)

    def execute(self,*args,**kwargs):
        raise NotImplemented('Every command must implement this function')
    
    def get_argument(self,argument:str):
        default_value = f'<{argument}_value>'
        return self.extra_args.get(argument,default_value)
    

    def get_templates(self):
        template_files = []
        template = self.get_argument('template')
        print(f'loading template {template}')
        for template_dir in self.template_dirs:
            template_dir = template_dir + '/' + template
            
            if os.path.isdir(template_dir):
                print(f'searching: {template_dir}')
                template_files += [os.path.join(template_dir, f) for f in os.listdir(template_dir) if f.endswith('.tmpl')]
                print(f'files: {template_files}')
            elif os.path.isfile(template_dir) and template_dir.endswith('.tmpl'):
                template_files.append(template_dir)
            else:
                print(f"can't find templates for {template}")
        return template_files

class ParseTemplateCommand(BorderlinerCommand):
    def __init__(self, name, template_path,*args,**kwargs):
        super().__init__(name,*args,**kwargs)
        self.template_path = template_path
        
    def save_parsed_templates(self, template_files):
        output_dir = self.get_argument('pipeline_name')
        os.makedirs(output_dir, exist_ok=True)
        for template_file in template_files:
            with open(template_file, 'r') as f:
                template = f.read()
            parsed_template = self.parse_template(template, self.args)

            output_file = os.path.join(output_dir, self.get_argument('pipeline_name') + '_' + os.path.splitext(os.path.basename(template_file))[0])
            with open(output_file, 'w') as f:
                f.write(parsed_template)

            print(f"Parsed template {template_file} and saved to {output_file}")

    def execute(self):
        # Get the list of template files to parse
        template_files = self.get_templates()
        if not template_files:
            print(f"No template files found in {self.template_path}")
            return

        # Parse each template and save the results to a file
        self.save_parsed_templates(template_files)

    def parse_template(self, template, args):
        parsed_template = template
        for key, value in self.extra_args.items():
            
            if value:
                parsed_template = parsed_template.replace(f"{{{key}}}", value)
        return parsed_template