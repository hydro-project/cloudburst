import os

import torch
import torchvision

tpath = os.path.join(os.getenv('TORCH_HOME'), 'checkpoints')

resnet = torchvision.models.resnet101(pretrained=True)
resnet.eval()
torch.save(resnet, os.path.join(tpath, 'resnet101.model'))

incept = torchvision.models.inception_v3(pretrained=True)
incept.eval()
torch.save(resnet, os.path.join(tpath, 'inception_v3.model'))

alexnet = torchvision.models.alexnet(pretrained=True)
alexnet.eval()
torch.save(resnet, os.path.join(tpath, 'alexnet.model'))
